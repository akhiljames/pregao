package test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoFloat32OrFloat64Enforcement enforces the strict architectural directive:
// "The use of float32 or float64 is strictly banned. You must use shopspring/decimal for all financial logic."
// This test parses all production Go source files in the pregao repository and ensures no float types are used.
func TestNoFloat32OrFloat64Enforcement(t *testing.T) {
	rootDirs := []string{"../internal", "../cmd"}

	var violations []string

	for _, rootDir := range rootDirs {
		err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			fset := token.NewFileSet()
			node, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
			if err != nil {
				return err
			}

			ast.Inspect(node, func(n ast.Node) bool {
				ident, ok := n.(*ast.Ident)
				if !ok {
					return true
				}

				if ident.Name == "float32" || ident.Name == "float64" {
					pos := fset.Position(ident.Pos())
					violations = append(violations, pos.String())
				}
				return true
			})

			return nil
		})
		require.NoError(t, err, "Failed walking directory %s", rootDir)
	}

	assert.Empty(t, violations, "Strict Ban Violation: float32 or float64 discovered in production code: %v", violations)
}
