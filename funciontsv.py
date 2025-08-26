import pandas as pd
import sys
import ast


def listsFromTSV(path="C:\Users\cmore\icn\practicas_icn\monitoring_DB.tsv", run_ids=None):
    """Parse the TSV file and return lists of selected columns.

    Parameters
    ----------
    path : str, optional
        Path to the TSV file. Defaults to the original location used by the
        script.
    run_ids : int | list[int] | None, optional
        ``RUNID`` or collection of ``RUNID`` values to select. If ``None`` all
        rows are processed.

    Returns
    -------
    tuple[list[int], list[list[int]], list[list[float]]]
        ``listaMCMid`` – list of MCMID values,
        ``listaDataOK`` – list of integer lists from the "Data OK" column,
        ``listaGain`` – list of float lists from the "Gain" column.
    """

    # Read TSV and normalize header spacing
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    df = df.rename(columns={c: c.strip() for c in df.columns})

    # Resolve columns even if spacing/case differ
    def resolve(name: str) -> str:
        key = name.strip().lower().replace(" ", "")
        for c in df.columns:
            if c.strip().lower().replace(" ", "") == key:
                return c
        raise KeyError(f"Column '{name}' not found. Got: {list(df.columns)}")

    run_col = resolve("RUNID")
    mcm_col = resolve("MCMID")
    dok_col = resolve("Data OK")
    gain_col = resolve("Gain")  # 'Gain ' with trailing space is handled

    # Optionally filter by RUNID
    if run_ids is not None:
        if isinstance(run_ids, (int, float, str)):
            run_ids = [run_ids]
        try:
            run_ids = {int(str(r).strip()) for r in run_ids}
        except Exception:
            run_ids = {r for r in run_ids}
        run_series = pd.to_numeric(df[run_col].str.strip(), errors="coerce")
        df = df.loc[run_series.isin(run_ids)]

    # Helper to parse a stringified list like "[1, 0, 1, ...]"
    def parse_list(s: str) -> list:
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

    listaDataOK: list[list[int]] = []
    for cell in df[dok_col].tolist():
        vals: list[int] = []
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

    listaGain: list[list[float]] = []
    for cell in df[gain_col].tolist():
        vals: list[float] = []
        for x in parse_list(cell):
            try:
                vals.append(float(str(x).replace(",", ".")))
            except Exception:
                pass  # ignore non-numerics
        listaGain.append(vals)

    return listaMCMid, listaDataOK, listaGain


if __name__ == "__main__":
    arg_path = sys.argv[1] if len(sys.argv) > 1 else None
    mcmid, data_ok, gain = listsFromTSV(arg_path,49)

    print("listaMCMid:", mcmid)
    print("listaDataOK:", data_ok)
    print("listaGain:", gain)


# Example usage:
# listaMCMid, listaDataOK, listaGain = listsFromTSV(run_ids=211)
# mcmid_first_row = listaMCMid[0]
# dataok_first_row = listaDataOK[0]
# gain_first_row = listaGain[0]

