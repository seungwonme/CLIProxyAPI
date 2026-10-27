package adapter_test

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const transcriptModule = "github.com/router-for-me/CLIProxyAPI/v8"

func TestTranscriptDependencyBoundary(t *testing.T) {
	root := transcriptRepositoryRoot(t)
	transcriptRoot := filepath.Join(root, "internal", "transcript")
	coreFiles := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == filepath.Join(root, ".git") || path == filepath.Join(root, "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		rel, errRel := filepath.Rel(root, path)
		if errRel != nil {
			return errRel
		}
		rel = filepath.ToSlash(rel)
		inTranscript := strings.HasPrefix(rel, "internal/transcript/")
		if inTranscript && entry.Name() == "go.mod" {
			t.Errorf("%s introduces a second module inside the transcript tree", rel)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, errParse := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if errParse != nil {
			return errParse
		}
		inCore := strings.HasPrefix(rel, "internal/transcript/core/")
		if inCore {
			coreFiles++
		}
		for _, spec := range file.Imports {
			importPath, errImport := strconv.Unquote(spec.Path.Value)
			if errImport != nil {
				return errImport
			}
			if inCore {
				pkg, errResolve := build.Default.Import(importPath, "", build.FindOnly)
				if errResolve != nil || pkg == nil || !pkg.Goroot {
					t.Errorf("%s imports %q: core production imports must resolve in GOROOT (error: %v)", rel, importPath, errResolve)
				}
			}
			if inTranscript || (importPath != transcriptModule+"/internal/transcript" && !strings.HasPrefix(importPath, transcriptModule+"/internal/transcript/")) {
				continue
			}
			if rel != "internal/api/server.go" || importPath != transcriptModule+"/internal/transcript/adapter" {
				t.Errorf("%s imports %q: only internal/api/server.go may compose the adapter; native packages must use the logging/access/transport seams", rel, importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if coreFiles == 0 {
		t.Fatalf("no production Go files found in %s/core; cannot verify the dependency boundary", transcriptRoot)
	}
}

func TestTranscriptServerCompositionBypassesNativeLogger(t *testing.T) {
	root := transcriptRepositoryRoot(t)
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "internal", "api", "server.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	adapterAlias := transcriptImportAlias(t, file, transcriptModule+"/internal/transcript/adapter", "adapter")
	middlewareAlias := transcriptImportAlias(t, file, transcriptModule+"/internal/api/middleware", "middleware")
	var constructor *ast.FuncDecl
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv == nil && function.Name.Name == "NewServer" {
			constructor = function
		}
	}
	if constructor == nil || constructor.Body == nil {
		t.Fatal("NewServer implementation not found")
	}
	adapterCalls, transcriptMiddlewareCalls := 0, 0
	factoryCalls, nativeMiddlewareCalls := 0, 0
	var inspect func(ast.Node, bool)
	inspect = func(node ast.Node, disabledOnly bool) {
		if node == nil {
			return
		}
		ast.Inspect(node, func(child ast.Node) bool {
			switch expression := child.(type) {
			case *ast.IfStmt:
				if expression.Init != nil {
					inspect(expression.Init, disabledOnly)
				}
				inspect(expression.Cond, disabledOnly)
				inspect(expression.Body, disabledOnly || transcriptConditionDisablesCapture(expression.Cond))
				if expression.Else != nil {
					inspect(expression.Else, disabledOnly)
				}
				return false
			case *ast.CallExpr:
				if transcriptSelector(expression.Fun, adapterAlias, "New") {
					adapterCalls++
					if len(expression.Args) != 1 || !transcriptSelector(expression.Args[0], "cfg", "Transcripts") {
						t.Error("adapter.New must receive the native startup cfg.Transcripts")
					}
				}
				if selector, ok := expression.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Middleware" && transcriptSelector(selector.X, "s", "transcripts") {
					transcriptMiddlewareCalls++
				}
				isFactory := transcriptSelector(expression.Fun, "optionState", "requestLoggerFactory")
				isMiddleware := transcriptSelector(expression.Fun, middlewareAlias, "RequestLoggingMiddleware")
				if isFactory {
					factoryCalls++
				}
				if isMiddleware {
					nativeMiddlewareCalls++
				}
				if (isFactory || isMiddleware) && !disabledOnly {
					t.Error("native request logger factory and middleware must be unreachable when cfg.Transcripts.Enabled is true")
				}
			}
			return true
		})
	}
	inspect(constructor.Body, false)
	if adapterCalls != 1 || transcriptMiddlewareCalls != 1 {
		t.Errorf("NewServer must compose one real adapter and middleware; got %d adapter calls and %d middleware calls", adapterCalls, transcriptMiddlewareCalls)
	}
	if factoryCalls == 0 || nativeMiddlewareCalls == 0 {
		t.Error("disabled feature path must retain the native logger factory and request logging middleware")
	}
}

func transcriptRepositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		contents, errRead := os.ReadFile(filepath.Join(directory, "go.mod"))
		if errRead == nil {
			for _, line := range strings.Split(string(contents), "\n") {
				fields := strings.Fields(line)
				if len(fields) >= 2 && fields[0] == "module" {
					if strings.Trim(fields[1], "\"") != transcriptModule {
						t.Fatalf("unexpected module in %s/go.mod; review the transcript import guard", directory)
					}
					return directory
				}
			}
			t.Fatalf("module directive missing in %s/go.mod", directory)
		}
		if !os.IsNotExist(errRead) {
			t.Fatal(errRead)
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository go.mod not found")
		}
		directory = parent
	}
}

func transcriptImportAlias(t *testing.T, file *ast.File, importPath, defaultAlias string) string {
	t.Helper()
	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if value != importPath {
			continue
		}
		if spec.Name == nil {
			return defaultAlias
		}
		if spec.Name.Name == "." || spec.Name.Name == "_" {
			t.Fatalf("composition import %s must have an inspectable package name", importPath)
		}
		return spec.Name.Name
	}
	t.Fatalf("composition import %s not found", importPath)
	return ""
}

func transcriptSelector(expression ast.Expr, receiver, member string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != member {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == receiver
}

func transcriptConditionDisablesCapture(expression ast.Expr) bool {
	switch condition := expression.(type) {
	case *ast.ParenExpr:
		return transcriptConditionDisablesCapture(condition.X)
	case *ast.BinaryExpr:
		if condition.Op == token.LAND {
			return transcriptConditionDisablesCapture(condition.X) || transcriptConditionDisablesCapture(condition.Y)
		}
		if condition.Op == token.LOR {
			return transcriptConditionDisablesCapture(condition.X) && transcriptConditionDisablesCapture(condition.Y)
		}
	case *ast.UnaryExpr:
		if condition.Op == token.NOT {
			selector, ok := condition.X.(*ast.SelectorExpr)
			return ok && selector.Sel.Name == "Enabled" && transcriptSelector(selector.X, "cfg", "Transcripts")
		}
	}
	return false
}
