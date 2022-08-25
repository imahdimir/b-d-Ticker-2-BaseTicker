##

"""

  """

##


import pandas as pd
from githubdata import GithubData


btic_repo_url = 'https://github.com/imahdimir/d-uniq-BaseTickers'
map_repo_url = 'https://github.com/imahdimir/d-Ticker-2-BaseTicker-map'

btic = 'BaseTicker'
tick = 'Ticker'

ptrns = {
    0    : lambda x : x ,
    1    : lambda x : f'{x}1' ,
    2    : lambda x : f'{x}2' ,
    3    : lambda x : f'{x}3' ,
    4    : lambda x : f'{x}4' ,
    'h'  : lambda x : f'{x}ح' ,
    'h1' : lambda x : f'{x}ح' + '1' ,
    'h2' : lambda x : f'{x}ح' + '2' ,
    'h3' : lambda x : f'{x}ح' + '3' ,
    'h4' : lambda x : f'{x}ح' + '4' ,
    }

def main() :
  pass

  ##

  btics = GithubData(btic_repo_url)
  btics.clone()
  ##
  bdfpn = btics.data_filepath
  bdf = pd.read_excel(bdfpn)

  bdf = bdf.reset_index()
  bdf = bdf[[btic]]
  ##
  odf = pd.DataFrame()

  for _ , vl in ptrns.items() :
    _df = bdf.copy()
    _df[tick] = _df[btic].apply(vl)

    odf = pd.concat([odf , _df])

  ##
  odf = odf[[tick , btic]]
  ##
  odf = odf.sort_values([btic , tick])
  ##
  odf = odf.drop_duplicates()
  ##
  msk = odf.duplicated(subset = tick , keep = False)
  df1 = odf[msk]
  ##
  odf = odf[~ msk]
  ##

  tic2btic = GithubData(map_repo_url)
  tic2btic.clone()
  ##
  ofp = tic2btic.data_filepath
  ##
  odf = odf.set_index(tick)
  ##
  odf.to_parquet(ofp)
  ##
  commit_msg = 'fixed tickers that maps 2 baseticker , بورس3 , کالا1'
  tic2btic.commit_push(commit_msg)
  ##

  btics.rmdir()
  tic2btic.rmdir()

  ##


##