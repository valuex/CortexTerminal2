package sessions

import "regexp"

// ArtifactFilenameValidator is the Go port of
// CortexTerminal.Contracts.Sessions.ArtifactFilenameValidator. The C#
// uses a blacklist of unsafe characters / patterns; this implementation
// mirrors the regex set 1:1.
//
// MUST stay in sync with the Worker daemon — a previous divergent copy
// in the Worker used an ASCII-only whitelist that silently rejected
// non-ASCII filenames (e.g. Chinese); keeping a single source of truth
// prevents that drift from recurring. Future port: also import this
// from the Worker side via the same package.
//
// Allowed: Unicode letters/digits, interior spaces, common punctuation
// (parens, dashes, dots, etc.).
// Forbidden: control chars (0x00–0x1f), path separators, ':<|?*"',
// "..", leading/trailing '.' or ' ', Windows reserved names.
//
// On failure, the returned reason is the English description of the
// first failing rule — kept English on purpose so log/console encoding
// cannot garble the diagnostic.
var (
	unsafeChars   = regexp.MustCompile(`[\x00-\x1f/\\:<>|?*"]`)
	dotTraversal  = regexp.MustCompile(`\.\.`)
	edgeDotSpace  = regexp.MustCompile(`^[. ]|[. ]$`)
	windowsRsvdRe = regexp.MustCompile(`(?i)^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\.|$)`)
)

// ValidateFilename returns "" when filename passes every rule, otherwise
// the English reason for the first failing rule. Caller surfaces this to
// the client as a 400-equivalent error.
func ValidateFilename(filename string) string {
	if filename == "" {
		return "filename is empty"
	}
	if len(filename) > 255 {
		return "filename exceeds 255 characters"
	}
	if unsafeChars.MatchString(filename) {
		return "filename contains forbidden characters (control chars or one of / \\ : < > | ? * \")"
	}
	if dotTraversal.MatchString(filename) {
		return "filename contains '..'"
	}
	if edgeDotSpace.MatchString(filename) {
		return "filename starts or ends with '.' or space"
	}
	if windowsRsvdRe.MatchString(filename) {
		return "filename is a Windows reserved name (CON, PRN, AUX, NUL, COM1-9, LPT1-9)"
	}
	return ""
}