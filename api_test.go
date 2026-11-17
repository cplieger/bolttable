package bolttable

import (
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// apiGolden is v1.1.1's API, written by this test's own walker run in an
// export of that tag (regenCmd).
const (
	apiGolden = "testdata/api-v1.1.1.golden"
	regenCmd  = "git archive v1.1.1 | tar -x -C <dir>, copy api_test.go in, then UPDATE_GOLDEN=1 go test -run TestBackupSignatureUnchanged in <dir> and copy " + apiGolden + " back"
)

// TestBackupSignatureUnchanged checks that the package's API keeps every
// declaration of v1.1.1, and that Live.Backup is DB.Backup's signature.
func TestBackupSignatureUnchanged(t *testing.T) {
	got := exportedAPI(t, ".")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(apiGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(apiGolden, []byte(strings.Join(got, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(apiGolden)
	if err != nil {
		t.Fatalf("%v; regenerate: %s", err, regenCmd)
	}
	want := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	have := map[string]bool{}
	for _, l := range got {
		have[l] = true
	}
	for _, l := range want {
		if !have[l] {
			t.Errorf("v1.1.1 declaration changed or removed: %s (a golden written from another tree: %s)", l, regenCmd)
		}
	}
	const dbBackup = "method (*DB).Backup func(context.Context, string) error"
	if !slices.Contains(want, dbBackup) || !have[dbBackup] {
		t.Errorf("golden or tree lacks %q", dbBackup)
	}
	if live := "method (*Live).Backup func(context.Context, string) error"; !have[live] {
		t.Errorf("tree lacks %q", live)
	}
}

// exportedAPI lists every exported declaration of the package in dir, one
// line each, sorted. A signature drops its parameter names, which no caller
// depends on; an interface is one line, so adding a method changes it.
func exportedAPI(t *testing.T, dir string) []string {
	t.Helper()
	bp, err := build.ImportDir(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(bp.GoFiles))
	for _, name := range bp.GoFiles {
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	pkg, err := conf.Check(bp.ImportPath, fset, files, nil)
	if err != nil {
		t.Fatal(err)
	}
	q := types.RelativeTo(pkg)
	str := func(typ types.Type) string { return types.TypeString(typ, q) }
	var out []string
	for _, name := range pkg.Scope().Names() {
		obj := pkg.Scope().Lookup(name)
		if !obj.Exported() {
			continue
		}
		switch o := obj.(type) {
		case *types.Func:
			out = append(out, "func "+name+typeParams(o.Signature().TypeParams(), str)+" "+str(unnamed(o.Signature())))
		case *types.Var:
			out = append(out, "var "+name+" "+str(o.Type()))
		case *types.Const:
			out = append(out, "const "+name+" "+str(o.Type())+" = "+o.Val().ExactString())
		case *types.TypeName:
			out = append(out, typeLines(o, str)...)
		}
	}
	slices.Sort(out)
	return out
}

func typeLines(o *types.TypeName, str func(types.Type) string) []string {
	name := o.Name()
	named, ok := o.Type().(*types.Named)
	if o.IsAlias() || !ok {
		return []string{"type " + name + " = " + str(o.Type())}
	}
	head := "type " + name + typeParams(named.TypeParams(), str)
	var out []string
	switch u := named.Underlying().(type) {
	case *types.Struct:
		out = append(out, head+" struct")
		for f := range u.Fields() {
			if f.Exported() {
				out = append(out, "field "+name+"."+f.Name()+" "+str(stripNames(f.Type())))
			}
		}
	case *types.Interface:
		out = append(out, head+" "+str(unnamedInterface(u)))
	default:
		out = append(out, head+" "+str(u))
	}
	ms := types.NewMethodSet(types.NewPointer(named))
	for sel := range ms.Methods() {
		m := sel.Obj().(*types.Func)
		if !m.Exported() {
			continue
		}
		recv := "(*" + name + ")"
		if _, ptr := m.Signature().Recv().Type().(*types.Pointer); !ptr {
			recv = "(" + name + ")"
		}
		out = append(out, "method "+recv+"."+m.Name()+" "+str(unnamed(m.Signature())))
	}
	return out
}

func typeParams(tp *types.TypeParamList, str func(types.Type) string) string {
	if tp.Len() == 0 {
		return ""
	}
	params := make([]string, tp.Len())
	for i := range tp.Len() {
		params[i] = tp.At(i).Obj().Name() + " " + str(tp.At(i).Constraint())
	}
	return "[" + strings.Join(params, ", ") + "]"
}

// stripNames drops the parameter names of a function type.
func stripNames(typ types.Type) types.Type {
	if sig, ok := typ.(*types.Signature); ok {
		return unnamed(sig)
	}
	return typ
}

// unnamed is sig without its receiver, its type parameters and its
// parameter names, at every function type among its parameters and results.
func unnamed(sig *types.Signature) *types.Signature {
	strip := func(tup *types.Tuple) *types.Tuple {
		vars := make([]*types.Var, tup.Len())
		for i := range tup.Len() {
			vars[i] = types.NewParam(token.NoPos, nil, "", stripNames(tup.At(i).Type()))
		}
		return types.NewTuple(vars...)
	}
	return types.NewSignatureType(nil, nil, nil, strip(sig.Params()), strip(sig.Results()), sig.Variadic())
}

func unnamedInterface(it *types.Interface) *types.Interface {
	methods := make([]*types.Func, 0, it.NumExplicitMethods())
	for m := range it.ExplicitMethods() {
		methods = append(methods, types.NewFunc(token.NoPos, m.Pkg(), m.Name(), unnamed(m.Signature())))
	}
	embeds := make([]types.Type, 0, it.NumEmbeddeds())
	for e := range it.EmbeddedTypes() {
		embeds = append(embeds, e)
	}
	return types.NewInterfaceType(methods, embeds).Complete()
}
