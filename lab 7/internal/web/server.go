// Lab 7: Implement a web server

package web

import (
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type server struct {
	Addr string
	Port int

	metadataService VideoMetadataService
	contentService  VideoContentService

	mux *http.ServeMux
}

func NewServer(
	metadataService VideoMetadataService,
	contentService VideoContentService,
) *server {
	return &server{
		metadataService: metadataService,
		contentService:  contentService,
	}
}

func (s *server) Start(lis net.Listener) error {
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/upload", s.handleUpload)
	s.mux.HandleFunc("/videos/", s.handleVideo)
	s.mux.HandleFunc("/content/", s.handleVideoContent)
	s.mux.HandleFunc("/", s.handleIndex)

	return http.Serve(lis, s.mux)
}

var (
	indexTmpl = template.Must(template.New("index").Parse(indexHTML))
	videoTmpl = template.Must(template.New("video").Parse(videoHTML))
)

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	videos, err := s.metadataService.List()
	if err != nil {
		http.Error(w, "Failed to read video list", http.StatusInternalServerError)
		return
	}
	if err := indexTmpl.Execute(w, videos); err != nil {
		log.Println("Template execute error: ", err)
	}
}

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(10 << 30); err != nil {
		http.Error(w, "Could not parse form", http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to get file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	filename := header.Filename
	videoId := strings.TrimSuffix(filename, filepath.Ext(filename))
	if _, err := s.metadataService.Read(videoId); err == nil {
		http.Error(w, "Video ID already exists", http.StatusConflict)
		return
	} else if !strings.Contains(err.Error(), "no rows in result set") {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tmpDir := os.TempDir()
	tmpIn := filepath.Join(tmpDir, filename)
	outDir := filepath.Join(tmpDir, videoId)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		http.Error(w, "Failed to create tmp dir", http.StatusInternalServerError)
		return
	}

	f, err := os.Create(tmpIn)
	if err != nil {
		http.Error(w, "Failed to save tmp file", http.StatusInternalServerError)
		return
	}
	defer f.Close()
	if _, err := io.Copy(f, file); err != nil {
		http.Error(w, "Failed to write to tmp file", http.StatusInternalServerError)
		return
	}

	manifestPath := filepath.Join(outDir, "manifest.mpd")

	cmd := exec.Command("ffmpeg",
		"-i", tmpIn, // input file
		"-c:v", "libx264", // video codec
		"-c:a", "aac", // audio codec
		"-bf", "1", // max 1 b-frame
		"-keyint_min", "120", // minimum keyframe interval
		"-g", "120", // keyframe every 120 frames
		"-sc_threshold", "0", // scene change threshold
		"-b:v", "3000k", // video bitrate
		"-b:a", "128k", // audio bitrate
		"-f", "dash", // dash format
		"-use_timeline", "1", // use timeline
		"-use_template", "1", // use template
		"-init_seg_name", "init-$RepresentationID$.m4s", // init segment naming
		"-media_seg_name", "chunk-$RepresentationID$-$Number%05d$.m4s", // media segment naming
		"-seg_duration", "4", // segment duration in seconds
		manifestPath) // output file
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Println("ffmpeg output: ", string(out))
		http.Error(w, "FFmpeg conversion failed", http.StatusInternalServerError)
		return
	}

	entries, err := os.ReadDir(outDir)
	if err != nil {
		http.Error(w, "Failed to read out dir", http.StatusInternalServerError)
		return
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(outDir, ent.Name()))
		if err != nil {
			http.Error(w, "Failed to read segment file", http.StatusInternalServerError)
			return
		}
		if err := s.contentService.Write(videoId, ent.Name(), data); err != nil {
			http.Error(w, "Failed to write segment file", http.StatusInternalServerError)
			return
		}
	}

	if err := s.metadataService.Create(videoId, time.Now()); err != nil {
		http.Error(w, "Failed to write metadata", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", "/")
	w.WriteHeader(http.StatusSeeOther)
}

func (s *server) handleVideo(w http.ResponseWriter, r *http.Request) {
	videoId := r.URL.Path[len("/videos/"):]
	log.Println("GET Video ID:", videoId)

	meta, err := s.metadataService.Read(videoId)
	if err != nil {
		http.Error(w, "Video not found", http.StatusNotFound)
		return
	}
	if err := videoTmpl.Execute(w, meta); err != nil {
		log.Println("Template execute error: ", err)
	}
}

func (s *server) handleVideoContent(w http.ResponseWriter, r *http.Request) {
	// parse /content/<videoId>/<filename>
	videoId := r.URL.Path[len("/content/"):]
	parts := strings.Split(videoId, "/")
	if len(parts) != 2 {
		http.Error(w, "Invalid content path", http.StatusBadRequest)
		return
	}
	videoId = parts[0]
	filename := parts[1]
	log.Println("GET Video ID:", videoId, ",Filename:", filename)

	data, err := s.contentService.Read(videoId, filename)
	if err != nil {
		http.Error(w, "Content not found", http.StatusInternalServerError)
		return
	}

	ext := filepath.Ext(filename)
	switch strings.ToLower(ext) {
	case ".mpd":
		w.Header().Set("Content-Type", "application/dash+xml")
	case ".m4s":
		w.Header().Set("Content-Type", "video/iso.segment")
	}
	w.Write(data)
}
