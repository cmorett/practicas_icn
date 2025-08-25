import pandas as pd
import ast
import os
import sys
from pathlib import Path    

def listsFromTSV(path: str | None = None):
    """
    regresa:
      - listaMCMid: [int, int, ...]
      - listaDataOK: [[int, ...], [int, ...], ...]
      - listaGain:   [[float, ...], [float, ...], ...]
    """

    if path is None:
        # Default to a `monitoring_DB.tsv` file located alongside this script
        path = os.path.join(os.path.dirname(__file__), "monitoring_DB.tsv")

    # Read TSV and normalize header spacing
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    df = df.rename(columns={c: c.strip() for c in df.columns})

    # Resolve columns even if spacing/case differ
    def resolve(name):
        key = name.strip().lower().replace(" ", "")
        for c in df.columns:
            if c.strip().lower().replace(" ", "") == key:
                return c
        raise KeyError(f"Column '{name}' not found. Got: {list(df.columns)}")

    mcm_col  = resolve("MCMID")
    dok_col  = resolve("Data OK")
    gain_col = resolve("Gain")  # 'Gain ' with trailing space is handled

    # Helper to parse a stringified list like "[1, 0, 1, ...]"
    def parse_list(s):
        s = (s or "").strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                v = ast.literal_eval(s)
                if isinstance(v, (list, tuple)):
                    return list(v)
            except Exception:
                pass
        # Fallback splitter
        return [x.strip() for x in s.strip("[]").split(",") if x.strip()]

    # Build outputs
    listaMCMid = (
        pd.to_numeric(df[mcm_col].str.strip(), errors="coerce")
          .dropna()
          .astype(int)
          .tolist()
    )

    listaDataOK = []
    for cell in df[dok_col].tolist():
        vals = []
        for x in parse_list(cell):
            try:
                vals.append(int(float(x)))
            except Exception:
                xs = str(x).strip().lower()
                if xs in {"true", "ok", "yes", "y", "t"}: 
                    vals.append(1)
                elif xs in {"false", "no", "n", "f"}:    
                    vals.append(0)
                # else: ignore
        listaDataOK.append(vals)

    listaGain = []
    for cell in df[gain_col].tolist():
        vals = []
        for x in parse_list(cell):
            try:
                vals.append(float(str(x).replace(",", ".")))
            except Exception:
                pass  # ignore non-numerics
        listaGain.append(vals)

    return listaMCMid, listaDataOK, listaGain

if __name__ == "__main__":
    # Allow an optional path argument; otherwise use the default.
    arg_path = sys.argv[1] if len(sys.argv) > 1 else None
    mcmid, data_ok, gain = listsFromTSV(arg_path)

    print("listaMCMid:", mcmid)
    print("listaDataOK:", data_ok)
    print("listaGain:", gain)


# Example usage:
# listaMCMid, listaDataOK, listaGain = listsFromTSV()
# mcmid_first_row = listaMCMid[0]
# dataok_first_row = listaDataOK[0]
# gain_first_row = listaGain[0]
