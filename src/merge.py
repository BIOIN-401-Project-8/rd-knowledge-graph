#%%
import pandas as pd


# %%
def get_rgd_df_pmids(rgd_csv: str):
    rgd_df = pd.read_csv(rgd_csv, usecols=["PMID", "article_path"], dtype={"PMID": int, "article_path": str})
    return rgd_df, set(rgd_df["PMID"])


rgd_pmids = get_rgd_df_pmids('/workspaces/data/pmc-open-access-subset/merged.csv')[1]
# %%
len(rgd_pmids)
# %%
rgd_pmids_2 = {
    pmid for pmid in rgd_pmids if pmid not in range(1,38060310 + 1)
}

# %%
len(rgd_pmids_2)
# %%
list(rgd_pmids_2)[:10]
# %%
