// Package linkbox provides an interface to the linkbox.to Cloud storage system.
package linkbox

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs" 
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/multipart"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	maxEntitiesPerPage = 1000; minSleep = 200 * time.Millisecond; maxSleep = 2 * time.Second
	pacerBurst = 1; linkboxAPIURL = "https://www.linkbox.to/api/open/"; teleboxAPIURL = "https://www.telebox.online/api/"
	rootID = "0"; defaultUploadCutoff = 50 * fs.Mebi; defaultChunkSize = 5 * fs.Mebi
	defaultUploadConcurrency = 4; minChunkSize = 5 * fs.Mebi
)

func init() { /* ... same ... */ 
	fsi := &fs.RegInfo{ Name: "linkbox", Description: "Linkbox", NewFs: NewFs,
		Options: []fs.Option{
			{ Name: "token", Help: "Token from https://www.linkbox.to/admin/account (or Telebox)", Sensitive: true, Required:  true, },
			{ Name: "upload_cutoff", Help: "Cutoff for switching to multipart upload.", Default: defaultUploadCutoff, Advanced: true, },
			{ Name: "chunk_size", Help: "Chunk size for multipart uploads (min 5 MiB).", Default: defaultChunkSize, Advanced: true, },
			{ Name: "upload_concurrency", Help: "Concurrency for multipart uploads.", Default: defaultUploadConcurrency, Advanced: true, },
		},
	}
	fs.Register(fsi)
}
type Options struct { Token string `config:"token"`; UploadCutoff fs.SizeSuffix `config:"upload_cutoff"`; ChunkSize fs.SizeSuffix `config:"chunk_size"`; UploadConcurrency int `config:"upload_concurrency"`}
type Fs struct { name string; root string; opt Options; features *fs.Features; ci *fs.ConfigInfo; srv *rest.Client; dirCache *dircache.DirCache; pacer *fs.Pacer }
type Object struct { fs *Fs; remote string; size int64; modTime time.Time; contentType string; fullURL string; dirID int64; itemID string; id int64; isDir bool }

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) { /* ... same ... */
	root = strings.Trim(root, "/"); opt := new(Options); err := configstruct.Set(m, opt)
	if err != nil { return nil, err }
	if opt.UploadCutoff <= 0 { opt.UploadCutoff = defaultUploadCutoff }
	if opt.ChunkSize <= 0 { opt.ChunkSize = defaultChunkSize }
	if opt.ChunkSize < minChunkSize { fs.Logf(name, "Chunk size %v < S3 min %v. Adjusting to %v.", opt.ChunkSize, minChunkSize, minChunkSize); opt.ChunkSize = minChunkSize }
	if opt.UploadCutoff < opt.ChunkSize { fs.Debugf(name, "Upload cutoff %v < chunk size %v. Forcing to chunk size.", opt.UploadCutoff, opt.ChunkSize); opt.UploadCutoff = opt.ChunkSize }
	if opt.UploadConcurrency <= 0 { opt.UploadConcurrency = defaultUploadConcurrency }
	ci := fs.GetConfig(ctx); baseHttpClient := fshttp.NewClient(ctx)
	f := &Fs{ name:  name, opt:   *opt, root:  root, ci:    ci, srv:   rest.NewClient(baseHttpClient), pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep))), }
	f.dirCache = dircache.New(root, rootID, f)
	f.features = (&fs.Features{ CanHaveEmptyDirectories: true, CaseInsensitive: true, OpenChunkWriter: f.OpenChunkWriter, }).Fill(ctx, f)
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f ; tempF.dirCache = dircache.New(newRoot, rootID, &tempF) ; tempF.root = newRoot
		if err = tempF.dirCache.FindRoot(ctx, false); err != nil { return f, nil }
		if _, err = tempF.NewObject(ctx, remote); err != nil { if err == fs.ErrorObjectNotFound { return f, nil }; return nil, err }
		f.features.Fill(ctx, &tempF) ; f.dirCache = tempF.dirCache ; f.root = tempF.root
		return f, fs.ErrorIsFile
	}
	return f, nil
}

type entity struct { Type string `json:"type"`; Name string `json:"name"`; URL string `json:"url"`; Ctime int64 `json:"ctime"`; Size int64 `json:"size"`; ID int64 `json:"id"`; Pid int64 `json:"pid"`; ItemID string `json:"item_id"`}
func (e *entity) isDir() bool { return e.Type == "dir" || e.Type == "sdir" }
type data struct{ Entities []entity `json:"list"` }
type fileSearchRes struct { response; SearchData data `json:"data"`}
type response struct { Message string `json:"msg"`; Status  int    `json:"status"`}
func (r *response) IsError() bool { return r.Status != 1 }
func (r *response) Error() string { return fmt.Sprintf("Linkbox API error %d: %s", r.Status, r.Message) }
type responser interface { IsError() bool; Error() string }
type FileUploadSessionData struct { AK string `json:"ak"`; Bucket string `json:"bucket"`; PoolPath string `json:"poolPath"`; SToken string `json:"sToken"`; Server string `json:"server"`; SK string `json:"sk"`; TS int64  `json:"ts"`}
type FileUploadSessionResponse struct { Data *FileUploadSessionData `json:"data"`; Status int `json:"status"`; Message string `json:"msg,omitempty"`}
func (r *FileUploadSessionResponse) IsError() bool { if r.Status == 1 { if r.Data != nil && r.Data.AK != "" && r.Data.Server != "" && r.Data.Bucket != "" && r.Data.PoolPath != "" { return false }; return true }; if r.Status == 600 { return false }; return true }
func (r *FileUploadSessionResponse) Error() string { if r.Status == 1 && (r.Data == nil || r.Data.AK == "") { return fmt.Sprintf("Telebox API success status (1) but S3 auth data is missing/incomplete. Message: '%s'", r.Message) }; return fmt.Sprintf("Telebox API error (Status: %d, Message: '%s')", r.Status, r.Message) }
func (r *FileUploadSessionResponse) GetS3AuthData() FileUploadSessionData { if r.Data != nil { return *r.Data }; return FileUploadSessionData{} }

func getS3Endpoint(s3Auth FileUploadSessionData) (string, error) { /* ... same ... */
	if s3Auth.Server == "" || s3Auth.Bucket == "" { return "", fmt.Errorf("missing server or bucket for S3 endpoint") }
	parsedServerURL, err := url.Parse(s3Auth.Server); if err != nil { return "", fmt.Errorf("parse S3 server URL '%s': %w", s3Auth.Server, err) }
	s3Host := s3Auth.Bucket + "." + parsedServerURL.Host; return parsedServerURL.Scheme + "://" + s3Host, nil
}
func newObsClientFromAuthData(authData FileUploadSessionData, constructedEndpoint string) (*obs.ObsClient, error) { /* ... same ... */
	if constructedEndpoint == "" || authData.AK == "" || authData.SK == "" { return nil, fmt.Errorf("incomplete credentials/endpoint for OBS client (endpoint, AK, or SK missing)") }
	var obsClient *obs.ObsClient; var err error
	if authData.SToken != "" { obsClient, err = obs.New(authData.AK, authData.SK, constructedEndpoint, obs.WithSecurityToken(authData.SToken))
	} else { obsClient, err = obs.New(authData.AK, authData.SK, constructedEndpoint) }
	if err != nil { return nil, fmt.Errorf("failed to initialize OBS client with endpoint '%s': %w", constructedEndpoint, err) }
	fs.Debugf(nil, "OBS Client initialized for endpoint: %s", constructedEndpoint); return obsClient, nil
}

func (o *Object) set(e *entity) { /* ... */ o.modTime = time.Unix(e.Ctime, 0); o.contentType = e.Type; o.size = e.Size; o.fullURL = e.URL; o.isDir = e.isDir(); o.id = e.ID; o.itemID = e.ItemID; o.dirID = e.Pid }
func getUnmarshaledTeleboxAPIResponse(ctx context.Context, f *Fs, opts *rest.Opts, result *FileUploadSessionResponse) error { /* ... */ 
	err := f.pacer.Call(func() (bool, error) {
		resp, httpErr := f.srv.CallJSON(ctx, opts, nil, result)
		if httpErr == nil { if result.IsError() { fs.Debugf(f, "Telebox API indicated error: Status: %d, Msg: '%s', Data: %+v", result.Status, result.Message, result.Data); return false, result }}
		return f.shouldRetry(ctx, resp, httpErr)
	}); if err != nil { return err }; if result.IsError() { return result }; return nil
}
func getUnmarshaledLinkboxAPIResponse(ctx context.Context, f *Fs, opts *rest.Opts, result responser) error { /* ... */ 
	err := f.pacer.Call(func() (bool, error) {
		resp, httpErr := f.srv.CallJSON(ctx, opts, nil, result)
		if httpErr == nil { if result.IsError() { return false, result }}
		return f.shouldRetry(ctx, resp, httpErr)
	}); if err != nil { return err }; if result.IsError() { return result }; return nil
}
var searchOK = regexp.MustCompile(`^[a-zA-Z0-9_ -.]{1,50}$`)
type listAllFn func(*entity) bool
func (f *Fs) listAll(ctx context.Context, dirID string, name string, fn listAllFn) (found bool, err error) { /* ... */ 
	var ( pageNumber = 0; numberOfEntities = maxEntitiesPerPage ); name = strings.TrimSpace(name); if !searchOK.MatchString(name) { name = "" }
OUTER:
	for numberOfEntities == maxEntitiesPerPage {
		pageNumber++; opts := &rest.Opts{ Method: "GET", RootURL: linkboxAPIURL, Path: "file_search", Parameters: url.Values{ "token": {f.opt.Token}, "name": {name}, "pid": {dirID}, "pageNo": {itoa(pageNumber)}, "pageSize": {itoa64(maxEntitiesPerPage)}, } }
		var res fileSearchRes; err = getUnmarshaledLinkboxAPIResponse(ctx, f, opts, &res)
		if err != nil { return false, fmt.Errorf("listAll failed: %w", err) }
		numberOfEntities = len(res.SearchData.Entities)
		for _, entity := range res.SearchData.Entities { if itoa64(entity.Pid) != dirID { continue }; if fn(&entity) { found = true; break OUTER } }
		if pageNumber > 100000 { return false, fmt.Errorf("listAll too many results") }
	}
	return found, nil
}
func itoa64(i int64) string { return strconv.FormatInt(i, 10) }
func itoa(i int) string     { return itoa64(int64(i)) }
func getEntity(ctx context.Context, f *Fs, leaf string, dirID string, token string) (*entity, error) { /* ... */ 
	var res *entity; resErr := fs.ErrorObjectNotFound
	_, err := f.listAll(ctx, dirID, leaf, func(e *entity) bool { if strings.EqualFold(e.Name, leaf) { if e.isDir() { res, resErr = nil, fs.ErrorIsDir } else { res, resErr = e, nil }; return true }; return false })
	if err != nil { return nil, err }; return res, resErr
}
func (f *Fs) FindLeaf(ctx context.Context, dirID, leaf string) (string, bool, error) { /* ... */ 
	var idOut string; found, err := f.listAll(ctx, dirID, leaf, func(e *entity) bool { if e.isDir() && strings.EqualFold(e.Name, leaf) { idOut = itoa64(e.ID); return true }; return false}); return idOut, found, err
}
type folderCreateRes struct { response; Data struct { DirID int64 `json:"dirId"`} `json:"data"`}
func (f *Fs) CreateDir(ctx context.Context, dirID, leaf string) (string, error) { /* ... */ 
	opts := &rest.Opts{ Method: "GET", RootURL: linkboxAPIURL, Path: "folder_create", Parameters: url.Values{ "token": {f.opt.Token}, "name": {leaf}, "pid": {dirID}, "isShare": {"0"}, "canInvite": {"1"}, "canShare": {"1"}, "withBodyImg": {"1"}, "desc": {""}, } }
	var res folderCreateRes; err := getUnmarshaledLinkboxAPIResponse(ctx, f, opts, &res)
	if err != nil { if res.IsError() && res.Status == 1501 { id, ok, ferr := f.FindLeaf(ctx, dirID, leaf); if ferr == nil && ok { return id, nil }; return "", fs.ErrorDirExists }; return "", fmt.Errorf("CreateDir failed: %w", err) }
	if res.Data.DirID == 0 { return "", fmt.Errorf("CreateDir API returned 0 ID") }; return itoa64(res.Data.DirID), nil
}
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) { /* ... */ 
	dirID, err := f.dirCache.FindDir(ctx, dir, false); if err != nil { return nil, err }
	_, err = f.listAll(ctx, dirID, "", func(e *entity) bool { remote := path.Join(dir, e.Name); if e.isDir() { id := itoa64(e.ID); mt := time.Unix(e.Ctime, 0); d := fs.NewDir(remote, mt).SetID(id).SetParentID(itoa64(e.Pid)); entries = append(entries, d); f.dirCache.Put(remote, id) } else { o := &Object{fs: f, remote: remote}; o.set(e); entries = append(entries, o) }; return false })
	if err != nil { return nil, err }; return entries, nil
}
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) { /* ... */ 
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false); if err != nil { if err == fs.ErrorDirNotFound { return nil, fs.ErrorObjectNotFound }; return nil, err }
	entity, err := getEntity(ctx, f, leaf, dirID, f.opt.Token); if err != nil { return nil, err }
	o := &Object{fs: f, remote: remote}; o.set(entity); return o, nil
}
func (f *Fs) Mkdir(ctx context.Context, dir string) error { /* ... */ _, err := f.dirCache.FindDir(ctx, dir, true); return err }
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error { /* ... */ 
	if check { entries, err := f.List(ctx, dir); if err != nil { return err }; if len(entries) != 0 { return fs.ErrorDirectoryNotEmpty } }
	dirID, err := f.dirCache.FindDir(ctx, dir, false); if err != nil { return err }
	opts := &rest.Opts{ Method: "GET", RootURL: linkboxAPIURL, Path: "folder_del", Parameters: url.Values{"token": {f.opt.Token}, "dirIds": {dirID}}, }
	var res response; err = getUnmarshaledLinkboxAPIResponse(ctx, f, opts, &res)
	if err != nil { if res.IsError() && (res.Status == 403 || res.Status == 500) { return fs.ErrorDirNotFound }; return fmt.Errorf("purge error: %w", err) }
	f.dirCache.FlushDir(dir); return nil
}
func (f *Fs) Rmdir(ctx context.Context, dir string) error { return f.purgeCheck(ctx, dir, true) }
func (o *Object) SetModTime(context.Context, time.Time) error { return fs.ErrorCantSetModTime }
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) { /* ... */ 
	var res *http.Response; downloadURL := o.fullURL
	if downloadURL == "" { _, name := dircache.SplitPath(o.Remote()); entity, err := getEntity(ctx, o.fs, name, itoa64(o.dirID), o.fs.opt.Token); if err != nil { return nil, err }; if entity == nil { return nil, fs.ErrorObjectNotFound }; downloadURL = entity.URL; o.fullURL = downloadURL }
	if downloadURL == "" { return nil, fs.ErrorObjectNotFound }
	opts := &rest.Opts{Method: "GET", RootURL: downloadURL, Options: options}
	err := o.fs.pacer.Call(func() (bool, error) { var e error; res, e = o.fs.srv.Call(ctx, opts); return o.fs.shouldRetry(ctx, res, e) })
	if err != nil { return nil, fmt.Errorf("Open failed: %w", err) }; return res.Body, nil
}
func (f *Fs) readFirst10MBAndPrepReader(ctx context.Context, in io.Reader, size int64) ([]byte, string, io.Reader, error) { 
	limit := int64(10 * 1024 * 1024); if size >= 0 && size < limit { limit = size }
	first10mBytesRead := make([]byte, int(limit))
	n, readErr := io.ReadFull(in, first10mBytesRead)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return nil, "", nil, fmt.Errorf("failed to read first 10MB: %w", readErr)
	}
	first10mBytesRead = first10mBytesRead[:n]; h := md5.Sum(first10mBytesRead); md5sum := fmt.Sprintf("%x", h)
	return first10mBytesRead, md5sum, io.MultiReader(bytes.NewReader(first10mBytesRead), in), nil
}
func (o *Object) completeUploadLinkbox(ctx context.Context, md5OfFirst10M string, size int64, remote string, options ...fs.OpenOption) error { /* ... */ 
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, remote, true); if err != nil { return fmt.Errorf("completeUploadLinkbox: FindPath failed: %w", err) }
	opts := &rest.Opts{ Method: "GET", RootURL: linkboxAPIURL, Path: "folder_upload_file", Options: options, Parameters: url.Values{ "token": {o.fs.opt.Token}, "fileMd5ofPre10m": {md5OfFirst10M}, "fileSize": {itoa64(size)}, "pid": {dirID}, "diyName": {leaf}, } }
	var res response; err = getUnmarshaledLinkboxAPIResponse(ctx, o.fs, opts, &res)
	if err != nil { return fmt.Errorf("completeUploadLinkbox: folder_upload_file failed: %w (status %d)", err, res.Status) }
	const maxTries = 10; var sleepTime = 200 * time.Millisecond; var entity *entity
	for try := 1; try <= maxTries; try++ {
		entity, err = getEntity(ctx, o.fs, leaf, dirID, o.fs.opt.Token)
		if err == nil { break }
		if err == fs.ErrorObjectNotFound && try < maxTries { time.Sleep(sleepTime); sleepTime *= 2; if sleepTime > 5*time.Second { sleepTime = 5*time.Second }; continue }
		return fmt.Errorf("completeUploadLinkbox: failed to read metadata post-upload: %w", err)
	}
	if err != nil { return fmt.Errorf("completeUploadLinkbox: failed to read metadata after retries: %w", err) }
	o.set(entity); return nil
}

// RcloneLinkboxUploadInfoOption is used to pass S3 auth data (and the object being updated)
// from Object.Update to Fs.OpenChunkWriter if Object.Update calls multipart.UploadMultipart.
// This is necessary because OpenChunkWriter might also be called directly by rclone core
// (e.g., for streaming uploads) in which case it needs to fetch this data itself.
type RcloneLinkboxUploadInfoOption struct {
	S3Auth         FileUploadSessionData // Carries AK, SK, SToken, Bucket, PoolPath, Server
	MD5ofPre10M    string                // Needed by linkboxChunkWriter for the final completeUploadLinkbox call
	ObjectToUpdate *Object               // The Fs.Object instance that is being updated
}
func (opt RcloneLinkboxUploadInfoOption) String() string { return "LinkboxUploadInfoOption" }
func (opt RcloneLinkboxUploadInfoOption) Header() (string, string) { return "", "" }
func (opt RcloneLinkboxUploadInfoOption) Mandatory() bool { return false }


func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	f := o.fs; size := src.Size()
	if size == 0 { return fs.ErrorCantUploadEmptyFiles }
	if size < 0 { return fmt.Errorf("linkbox: upload of unknown length files not supported") }

	if oldObj, errOld := f.NewObject(ctx, o.Remote()); errOld == nil {
		fs.Debugf(o, "Update: removing old file at %s", o.Remote())
		if errRem := oldObj.Remove(ctx); errRem != nil { fs.Errorf(o, "Update: failed to remove existing, proceeding: %v", errRem) }
	} else if errOld != fs.ErrorObjectNotFound { return fmt.Errorf("Update: error checking existing: %w", errOld) }

	_, md5Pre10M, streamReader, errRead := f.readFirst10MBAndPrepReader(ctx, in, size)
	if errRead != nil { return fmt.Errorf("Update: readFirst10MB: %w", errRead) }

	vgroup := fmt.Sprintf("%s_%d", md5Pre10M, size)
	fs.Debugf(o, "Update: Requesting upload session for %s, vgroup: %s", o.Remote(), vgroup)
	sessionOpts := &rest.Opts{ Method:  "GET", RootURL: teleboxAPIURL, Path: "file/get_file_upload_session", Parameters: url.Values{ "scene": {"common"}, "vgroupType": {"md5_10m"}, "vgroup": {vgroup}, "token": {f.opt.Token}, "platform": {"web"}, "pf": {"web"}, "lan": {"en"}, } }
	var sessionResp FileUploadSessionResponse
	errSession := getUnmarshaledTeleboxAPIResponse(ctx, f, sessionOpts, &sessionResp)

	if sessionResp.Status == 600 {
		fs.Debugf(o, "Update: Telebox API status 600 (msg: '%s') for %s: File content likely exists. Skipping S3 upload.", sessionResp.Message, o.Remote())
		return o.completeUploadLinkbox(ctx, md5Pre10M, size, src.Remote(), options...)
	}
	if errSession != nil { return fmt.Errorf("Update: get_file_upload_session for %s: %w", o.Remote(), errSession) }
	
	s3Auth := sessionResp.GetS3AuthData()
	fs.Debugf(o, "Update: S3AuthData for %s: %+v", o.Remote(), s3Auth)
	if s3Auth.Server == "" || s3Auth.Bucket == "" || s3Auth.PoolPath == "" || s3Auth.AK == "" {
		return fmt.Errorf("Update: incomplete S3 auth data for %s. Server: '%s', Bucket: '%s', Path: '%s', AK: '%s...'", o.Remote(), s3Auth.Server, s3Auth.Bucket, s3Auth.PoolPath, string([]rune(s3Auth.AK)[:min(3, len(s3Auth.AK))]))
	}
	
	s3Endpoint, errEndpoint := getS3Endpoint(s3Auth)
	if errEndpoint != nil { return fmt.Errorf("Update: getS3Endpoint for %s: %w", o.Remote(), errEndpoint) }
	s3ObjectKey := strings.TrimLeft(s3Auth.PoolPath, "/")

	obsClient, errSdk := newObsClientFromAuthData(s3Auth, s3Endpoint)
	if errSdk != nil { return fmt.Errorf("Update: failed to init OBS client for %s: %w", o.Remote(), errSdk) }
	
	if size < int64(f.opt.UploadCutoff) {
		fs.Debugf(o, "Using single part OBS PUT for %s (size %s < cutoff %s)", o.Remote(), fs.SizeSuffix(size), f.opt.UploadCutoff)
		defer obsClient.Close() 
		putInput := &obs.PutObjectInput{}
		putInput.Bucket = s3Auth.Bucket
		putInput.Key = s3ObjectKey
		putInput.Body = streamReader 
		putInput.ContentLength = size // This field IS present on obs.PutObjectInput according to SDK examples
		fs.Debugf(o, "OBS PutObject for %s: Bucket: %s, Key: %s, Size: %d", o.Remote(), putInput.Bucket, putInput.Key, putInput.ContentLength)
		_, errPut := obsClient.PutObject(putInput) 
		if errPut != nil { return fmt.Errorf("single part OBS PUT for %s failed: %w", o.Remote(), errPut) }
		return o.completeUploadLinkbox(ctx, md5Pre10M, size, src.Remote(), options...)
	}

	fs.Debugf(o, "Using multipart OBS upload for %s (size %s >= cutoff %s)", o.Remote(), fs.SizeSuffix(size), f.opt.UploadCutoff)
	obsClient.Close() // Close client from single-part path, multipart will manage its own.

	// Prepare the backend-specific option for OpenChunkWriter
	// This is crucial for when Update -> multipart.UploadMultipart -> OpenChunkWriter is the path.
	linkboxOpt := RcloneLinkboxUploadInfoOption{ 
		S3Auth:         s3Auth,
		MD5ofPre10M:    md5Pre10M, 
		ObjectToUpdate: o, // Pass the current object instance
	}
	// Prepend our specific option to any options passed down from the caller (e.g. operations layer)
	// Making it the first element can sometimes help in debugging if options are truncated.
	// Or, create a new slice if original `options` are not meant to be mixed.
	// The important part is that linkboxOpt is in the slice passed to UploadMultipart.
	finalMultipartOptions := make([]fs.OpenOption, 0, len(options)+1)
	finalMultipartOptions = append(finalMultipartOptions, linkboxOpt)
	finalMultipartOptions = append(finalMultipartOptions, options...)


	fs.Debugf(o, "Object.Update: About to call UploadMultipart for %s", src.Remote())
	fs.Debugf(o, "Object.Update: finalMultipartOptions being passed (len %d):", len(finalMultipartOptions))
	for i, optFromSlice := range finalMultipartOptions {
	    fs.Debugf(o, "Object.Update: finalMultipartOptions[%d]: Type=%T, Value=%+v", i, optFromSlice, optFromSlice)
	    if _, ok := optFromSlice.(RcloneLinkboxUploadInfoOption); ok {
	        fs.Debugf(o, "Object.Update: Found RcloneLinkboxUploadInfoOption at finalMultipartOptions[%d]", i)
	    }
	}
	
	_, errMulti := multipart.UploadMultipart(ctx, src, streamReader, multipart.UploadMultipartOptions{ Open: f, OpenOptions: finalMultipartOptions })
	if errMulti != nil { return fmt.Errorf("multipart.UploadMultipart for %s failed: %w", o.Remote(), errMulti) }
	return nil
}

func (o *Object) Remove(ctx context.Context) error { /* ... */ 
	opts := &rest.Opts{ Method: "GET", RootURL: linkboxAPIURL, Path: "file_del", Parameters: url.Values{"token": {o.fs.opt.Token}, "itemIds": {o.itemID}}, }
	var res response; err := getUnmarshaledLinkboxAPIResponse(ctx, o.fs, opts, &res)
	if err != nil { return fmt.Errorf("could not Remove %s: %w (status %d)", o.Remote(), err, res.Status) }; return nil
}
func (o *Object) ModTime(context.Context) time.Time { return o.modTime }
func (o *Object) Remote() string                   { return o.remote }
func (o *Object) Size() int64                      { return o.size }
func (o *Object) String() string { if o == nil { return "<nil>" }; return o.remote }
func (o *Object) Fs() fs.Info    { return o.fs }
func (o *Object) Hash(context.Context, hash.Type) (string, error) { return "", hash.ErrUnsupported }
func (o *Object) Storable() bool { return true }
func (f *Fs) Features() *fs.Features   { return f.features }
func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("Linkbox root '%s'", f.root) }
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.None) }
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) { /* ... */ 
	o := &Object{fs: f, remote: src.Remote(), size: src.Size()}
	dir, _ := dircache.SplitPath(src.Remote()); if dir != "" { err := f.Mkdir(ctx, dir); if err != nil { return nil, fmt.Errorf("Put: Mkdir %q: %w", dir, err) } }
	// Pass the options from Put to Update
	err := o.Update(ctx, in, src, options...)
	return o, err
}
func (f *Fs) Purge(ctx context.Context, dir string) error   { return f.purgeCheck(ctx, dir, false) }
func (f *Fs) DirCacheFlush()                                { f.dirCache.ResetRoot() }
var retryErrorCodes = []int{429, 500, 502, 503, 504, 509}
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) { /* ... */ 
	if fserrors.ContextError(ctx, &err) { return false, err }; return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// S3 XML Structs (unchanged)
type InitiateMultipartUploadResult struct { XMLName xml.Name `xml:"InitiateMultipartUploadResult"`; Bucket string `xml:"Bucket"`; Key string `xml:"Key"`; UploadID string `xml:"UploadId"`}
// S3Error might not be needed if SDK provides its own error types.

type linkboxChunkWriter struct {
	f             *Fs; o *Object; srcRemote     string
	s3Auth        FileUploadSessionData; obsClient *obs.ObsClient 
	s3ObjectKey   string; s3UploadID    string
	parts         []obs.Part; mu sync.Mutex 
	md5OfFirst10M string; fileSize int64 
}

func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (fs.ChunkWriterInfo, fs.ChunkWriter, error) {
	fs.Debugf(f, "Fs.OpenChunkWriter: Called for %s. Received options (len %d):", remote, len(options))
    for i, opt := range options { 
        fs.Debugf(f, "Fs.OpenChunkWriter: Option %d: Type=%T, Value=%+v", i, opt, opt)
        if _, ok := opt.(RcloneLinkboxUploadInfoOption); ok {
            fs.Debugf(f, "Fs.OpenChunkWriter: Found RcloneLinkboxUploadInfoOption at index %d in OpenChunkWriter", i)
        }
    }

	var s3Auth FileUploadSessionData
	var md5Pre10M string
	var objectToUpdate *Object
	foundCustomOpt := false

	for _, option := range options {
		if opt, ok := option.(RcloneLinkboxUploadInfoOption); ok {
			s3Auth = opt.S3Auth
			md5Pre10M = opt.MD5ofPre10M
			objectToUpdate = opt.ObjectToUpdate
			foundCustomOpt = true
			fs.Debugf(f, "OpenChunkWriter: Successfully extracted RcloneLinkboxUploadInfoOption for %s", remote)
			break
		}
	}

	if !foundCustomOpt {
		// This path is taken if rclone core calls OpenChunkWriter directly (e.g. streaming upload)
		// We MUST fetch credentials here.
		fs.Debugf(f, "OpenChunkWriter: RcloneLinkboxUploadInfoOption not found for %s. Fetching S3 session info.", remote)
		
		// Need to open the source to read its first 10MB for md5
		// This is problematic as fs.ObjectInfo doesn't guarantee Open()
		// This indicates a design issue if OpenChunkWriter can be called without pre-fetched S3Auth
		// for a backend that requires dynamic, content-based credentials.
		// For now, we will rely on the option being passed. If it's not, this will fail.
		// This path (direct call by rclone core) seems to be the one causing the issue.
		return fs.ChunkWriterInfo{}, nil, fmt.Errorf("OpenChunkWriter: RcloneLinkboxUploadInfoOption not found for %s - backend requires pre-fetched S3 credentials via this option", remote)
	}

	if s3Auth.Server == "" || s3Auth.Bucket == "" || s3Auth.PoolPath == "" {
		return fs.ChunkWriterInfo{}, nil, fmt.Errorf("OpenChunkWriter: Incomplete S3 auth data in option for %s", remote)
	}
	if objectToUpdate == nil {
		return fs.ChunkWriterInfo{}, nil, fmt.Errorf("OpenChunkWriter: ObjectToUpdate not found in RcloneLinkboxUploadInfoOption for %s", remote)
	}


	s3Endpoint, err := getS3Endpoint(s3Auth)
	if err != nil { return fs.ChunkWriterInfo{}, nil, fmt.Errorf("OpenChunkWriter: getS3Endpoint for %s: %w", remote, err) }
	s3ObjectKey := strings.TrimLeft(s3Auth.PoolPath, "/")

	obsClient, err := newObsClientFromAuthData(s3Auth, s3Endpoint)
	if err != nil { return fs.ChunkWriterInfo{}, nil, fmt.Errorf("OpenChunkWriter: newObsClientFromAuthData for %s: %w", remote, err) }

	initiateInput := &obs.InitiateMultipartUploadInput{}; initiateInput.Bucket = s3Auth.Bucket; initiateInput.Key = s3ObjectKey
	fs.Debugf(f, "OBS InitiateMultipartUpload for %s: Bucket: %s, Key: %s", remote, initiateInput.Bucket, initiateInput.Key)
	initiateOutput, err := obsClient.InitiateMultipartUpload(initiateInput)
	if err != nil { obsClient.Close(); return fs.ChunkWriterInfo{}, nil, fmt.Errorf("OpenChunkWriter: OBS InitiateMultipartUpload for %s failed: %w", remote, err) }
	if initiateOutput.UploadId == "" { obsClient.Close(); return fs.ChunkWriterInfo{}, nil, fmt.Errorf("OpenChunkWriter: OBS InitiateMultipartUpload for %s did not return UploadId", remote) }
	
	writer := &linkboxChunkWriter{
		f: f, o: objectToUpdate, srcRemote: remote, s3Auth: s3Auth, obsClient: obsClient,
		s3ObjectKey: s3ObjectKey, s3UploadID: initiateOutput.UploadId,
		parts: make([]obs.Part, 0), md5OfFirst10M: md5Pre10M, fileSize: src.Size(), // src.Size() is correct here
	}
	info := fs.ChunkWriterInfo{ ChunkSize: int64(f.opt.ChunkSize), Concurrency: f.opt.UploadConcurrency }
	return info, writer, nil
}


func (lcw *linkboxChunkWriter) WriteChunk(ctx context.Context, partNumber int, reader io.ReadSeeker) (int64, error) { /* ... same as before ... */
	size, err := reader.Seek(0, io.SeekEnd); if err != nil { return 0, fmt.Errorf("WriteChunk: seek end: %w", err) }
	_, err = reader.Seek(0, io.SeekStart); if err != nil { return 0, fmt.Errorf("WriteChunk: seek start: %w", err) }
	sdkPartNumber := partNumber + 1 
	uploadPartInput := &obs.UploadPartInput{}; uploadPartInput.Bucket = lcw.s3Auth.Bucket; uploadPartInput.Key = lcw.s3ObjectKey
	uploadPartInput.UploadId = lcw.s3UploadID; uploadPartInput.PartNumber = sdkPartNumber; uploadPartInput.Body = reader; uploadPartInput.PartSize = size 
	fs.Debugf(lcw.srcRemote, "OBS UploadPart: Bucket: %s, Key: %s, UploadID: %s, Part: %d, Size: %d", uploadPartInput.Bucket, uploadPartInput.Key, uploadPartInput.UploadId, uploadPartInput.PartNumber, uploadPartInput.PartSize)
	uploadPartOutput, err := lcw.obsClient.UploadPart(uploadPartInput)
	if err != nil { return 0, fmt.Errorf("WriteChunk: OBS UploadPart %d for %s failed: %w", sdkPartNumber, lcw.srcRemote, err) }
	if uploadPartOutput.ETag == "" { return 0, fmt.Errorf("WriteChunk: OBS UploadPart %d response missing ETag", sdkPartNumber) }
	lcw.mu.Lock(); lcw.parts = append(lcw.parts, obs.Part{PartNumber: uploadPartInput.PartNumber, ETag: uploadPartOutput.ETag}); lcw.mu.Unlock()
	return size, nil
}
func (lcw *linkboxChunkWriter) Close(ctx context.Context) error { /* ... same as before ... */
	defer func() { fs.Debugf(lcw.srcRemote, "Closing OBS client in chunkWriter.Close"); lcw.obsClient.Close() }()
	sort.Slice(lcw.parts, func(i, j int) bool { return lcw.parts[i].PartNumber < lcw.parts[j].PartNumber })
	completeInput := &obs.CompleteMultipartUploadInput{}; completeInput.Bucket = lcw.s3Auth.Bucket; completeInput.Key = lcw.s3ObjectKey
	completeInput.UploadId = lcw.s3UploadID; completeInput.Parts = lcw.parts 
	fs.Debugf(lcw.srcRemote, "OBS CompleteMultipartUpload: Bucket: %s, Key: %s, UploadID: %s, Parts: %d", completeInput.Bucket, completeInput.Key, completeInput.UploadId, len(completeInput.Parts))
	_, err := lcw.obsClient.CompleteMultipartUpload(completeInput) 
	if err != nil { return fmt.Errorf("Close: OBS CompleteMultipartUpload for %s failed: %w", lcw.srcRemote, err) }
	return lcw.o.completeUploadLinkbox(ctx, lcw.md5OfFirst10M, lcw.fileSize, lcw.srcRemote)
}
func (lcw *linkboxChunkWriter) Abort(ctx context.Context) error { /* ... same as before ... */
	defer func() { fs.Debugf(lcw.srcRemote, "Closing OBS client in chunkWriter.Abort"); lcw.obsClient.Close() }()
	fs.Debugf(lcw.srcRemote, "Aborting OBS multipart upload ID: %s for Key: %s", lcw.s3UploadID, lcw.s3ObjectKey)
	abortInput := &obs.AbortMultipartUploadInput{}; abortInput.Bucket = lcw.s3Auth.Bucket; abortInput.Key = lcw.s3ObjectKey
	abortInput.UploadId = lcw.s3UploadID
	_, err := lcw.obsClient.AbortMultipartUpload(abortInput) 
	if err != nil { return fmt.Errorf("Abort: OBS AbortMultipartUpload for %s failed: %w", lcw.srcRemote, err) }
	return nil
}

var (
	_ fs.Fs = &Fs{}; _ fs.Purger = &Fs{}; _ fs.DirCacheFlusher = &Fs{}; _ fs.OpenChunkWriter = &Fs{}
	_ fs.Object = &Object{}; _ fs.ChunkWriter = &linkboxChunkWriter{}
	_ responser = &response{}; _ responser = &FileUploadSessionResponse{}
)
func min(a, b int) int { if a < b { return a }; return b }