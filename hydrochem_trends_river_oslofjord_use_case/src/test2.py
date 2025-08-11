from pathlib import Path
import numpy as np
import xarray as xr

# paths to compare
pA = Path("../data/processed/cleaned_riverchem_40352TEST.nc")
pB = Path("../data/processed/river/cleaned_riverchem_40352.nc")

with xr.open_dataset(pA) as A, xr.open_dataset(pB) as B:
    print("A:", pA.resolve())
    print("B:", pB.resolve())

    # 1) Strict check (data + coords + attrs must match exactly)
    try:
        xr.testing.assert_identical(A, B)
        print("\n✅ IDENTICAL (including attributes).")
    except AssertionError as e:
        print("\n❌ Not identical:", e)

        # 2) If not identical, check numeric equality (attrs may differ)
        try:
            xr.testing.assert_allclose(A, B, rtol=1e-12, atol=0.0)
            print("✅ Data are numerically equal (attributes differ).")
        except AssertionError as e2:
            print("❌ Data differ too:", e2)

        # 3) Show detailed diffs

        # global attrs
        print("\n-- Global attrs that differ --")
        keys = sorted(set(A.attrs) | set(B.attrs))
        for k in keys:
            va, vb = A.attrs.get(k), B.attrs.get(k)
            if va != vb:
                print(f"  {k!r}: {va!r}  !=  {vb!r}")

        # coords & vars presence
        print("\n-- Coordinate variables --")
        print(" only in A:", sorted(set(A.coords) - set(B.coords)))
        print(" only in B:", sorted(set(B.coords) - set(A.coords)))

        print("\n-- Data variables --")
        print(" only in A:", sorted(set(A.data_vars) - set(B.data_vars)))
        print(" only in B:", sorted(set(B.data_vars) - set(A.data_vars)))

        # per-variable attrs & value diffs
        common = sorted(set(A.data_vars) & set(B.data_vars))
        for v in common:
            da, db = A[v], B[v]

            # attr diffs
            akeys = set(da.attrs); bkeys = set(db.attrs)
            diffs = {k: (da.attrs.get(k), db.attrs.get(k))
                     for k in (akeys | bkeys) if da.attrs.get(k) != db.attrs.get(k)}
            if diffs:
                print(f"\nVar '{v}' attribute diffs:")
                for k, (va, vb) in diffs.items():
                    print(f"  - {k}: {va!r}  !=  {vb!r}")

            # dims/shape/dtype
            if da.dims != db.dims or da.shape != db.shape or da.dtype != db.dtype:
                print(f"\nVar '{v}' structure differs:")
                print("  A:", da.dims, da.shape, da.dtype)
                print("  B:", db.dims, db.shape, db.dtype)

            # numeric diff (ignoring NaN==NaN)
            if da.shape == db.shape:
                Aa, Bb = da.values, db.values
                mask = ~(np.isnan(Aa) & np.isnan(Bb))
                if mask.any():
                    maxdiff = float(np.nanmax(np.abs(Aa[mask] - Bb[mask])))
                else:
                    maxdiff = 0.0
                if maxdiff != 0.0:
                    print(f"Var '{v}' max abs diff: {maxdiff}")

# from pathlib import Path
# import xarray as xr
# from pprint import pprint
#
# p = Path("../data/processed/cleaned_riverchem_40352TEST.nc")
#
# with xr.open_dataset(p) as ds:
#     print("Opened:", p.resolve())
#
#     dc = ds.attrs.get("date_created")
#     dm = ds.attrs.get("date_modified")
#     summary = ds.attrs.get("summary")        # added
#     summary_no = ds.attrs.get("summary_no")  # added
#
#     print("date_created :", dc)
#     print("date_modified:", dm)
#     print("summary      :", summary)         # added
#     print("summary_no   :", summary_no)      # added
#
#     if dc is None:
#         print("\n(date_created not found — here are all global attrs)")
#         pprint(dict(ds.attrs))
#
