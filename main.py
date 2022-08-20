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
  btics.clone_overwrite_last_version()
  ##
  bdfpn = btics.data_fps[0]
  bdf = pd.read_parquet(bdfpn)

  bdf = bdf.reset_index()
  bdf = bdf[[btic]]
  ##
  odf = pd.DataFrame()

  for _ , vl in ptrns.items() :
    bdf[tick] = bdf[btic].apply(vl)
    odf = pd.concat([odf , bdf])

  ##
  odf = odf.sort_values([btic , tick])
  ##

  tic2btic = GithubData(map_repo_url)
  tic2btic.clone_overwrite_last_version()
  ##
  ofp = tic2btic.data_fps[0]
  pre_odf = pd.read_parquet(ofp)

  pre_odf = pre_odf.reset_index()
  pre_odf = pre_odf[[tick, btic]]
  ##
  odf = pd.concat([odf , pre_odf])
  ##
  odf = odf.drop_duplicates()
  ##
  odf = odf[[tick, btic]]
  ##
  odf = odf.set_index(tick)
  ##
  odf.to_parquet(ofp)
  ##
  commit_msg = 'add by repo: https://github.com/imahdimir/build-Tickers-fr-BaseTickers-by-ptrn'
  tic2btic.commit_and_push_to_github_data_target(commit_msg)
  ##

  btics.rmdir()
  tic2btic.rmdir()

  ##

##


if __name__ == "__main__" :
  main()

# noinspection PyUnreachableCode
if False :
  pass

  ##

  ##

##