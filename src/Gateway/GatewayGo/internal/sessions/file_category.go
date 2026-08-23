package sessions

import (
	"path"
	"strings"
)

// File category constants mirror CortexTerminal.Contracts.Sessions.ArtifactFileCategory.
// They appear on the wire in MessagePack and must stay byte-stable.
const (
	CategoryImage   = "image"
	CategoryPdf     = "pdf"
	CategoryVideo   = "video"
	CategoryAudio   = "audio"
	CategoryArchive = "archive"
	CategoryCode    = "code"
	CategoryText    = "text"
	CategoryUnknown = "unknown"
)

// DetectFileCategory returns the category constant for filename. Extension
// lookup is case-insensitive and matches the C# FileCategoryDetector 1:1.
func DetectFileCategory(filename string) string {
	ext := strings.ToLower(path.Ext(filename))
	if ext == "" {
		return CategoryUnknown
	}
	if imageExts[ext] {
		return CategoryImage
	}
	if ext == ".pdf" {
		return CategoryPdf
	}
	if videoExts[ext] {
		return CategoryVideo
	}
	if audioExts[ext] {
		return CategoryAudio
	}
	if archiveExts[ext] {
		return CategoryArchive
	}
	if codeExts[ext] {
		return CategoryCode
	}
	if textExts[ext] {
		return CategoryText
	}
	return CategoryUnknown
}

var (
	imageExts = mkSet(".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg", ".ico", ".tiff", ".tif", ".heic")
	videoExts = mkSet(".mp4", ".mov", ".mkv", ".webm", ".avi", ".flv", ".wmv", ".m4v", ".3gp")
	audioExts = mkSet(".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a", ".wma", ".opus")
	archiveExts = mkSet(".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar", ".zst")
	codeExts = mkSet(
		".cs", ".ts", ".tsx", ".js", ".jsx", ".py", ".go", ".rs", ".java", ".kt", ".swift",
		".c", ".cpp", ".cc", ".h", ".hpp", ".rb", ".php", ".scala", ".lua", ".pl",
		".sh", ".bash", ".zsh", ".fish", ".ps1", ".bat", ".cmd",
		".sql", ".vue", ".svelte", ".html", ".css", ".scss", ".sass", ".less",
		".json", ".yaml", ".yml", ".toml", ".xml", ".ini", ".conf", ".env",
	)
	textExts = mkSet(".txt", ".md", ".markdown", ".rst", ".log", ".csv", ".tsv", ".rtf")
)

func mkSet(extensions ...string) map[string]bool {
	m := make(map[string]bool, len(extensions))
	for _, e := range extensions {
		m[e] = true
	}
	return m
}