##

"""

  """

##

import json
from pathlib import Path

import pandas as pd
from mirutil.funcs import save_as_prq_wo_index as sprq
from mirutil.funcs import read_data_according_to_type as rdata
from mirutil.funcs import norm_fa_str as norm
from githubdata import GithubData


btic = 'BaseTicker'
tick = 'Ticker'

btic_repo_url = 'https://github.com/imahdimir/d-Unique-BaseTickers-TSETMC'
map_repo_url = 'https://github.com/imahdimir/d-Ticker-2-BaseTicker-map'

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
  fpn = btics.data_fps[0]
  ##
  df = rdata(fpn)
  odf = pd.DataFrame()
  ##
  for _ , vl in ptrns.items() :
    df[tick] = df[btic].apply(vl)
    odf = pd.concat([odf , df])

  ##
  odf = odf.sort_values([btic , tick])
  ##

  tic2btic = GithubData(map_repo_url)
  tic2btic.clone_overwrite_last_version()
  ##
  tic2btic.set_data_fps()
  ##
  ofp = tic2btic.data_fps[0]
  ##
  pre_odf = pd.read_parquet(ofp)
  ##
  odf = pd.concat([odf , pre_odf])
  ##
  odf = odf.drop_duplicates()
  ##
  sprq(odf , ofp)
  ##
  tic2btic.commit_and_push_to_github_data_target(
      'init by repo: build-Tickers-fr-BaseTicers-by-ptrn')

  ##
  btics.rmdir()
  ##
  tic2btic.rmdir()

  ##

##


if __name__ == "__main__" :
  main()

# noinspection PyUnreachableCode
if False :
  pass

  ##
  sprq(odf , 'data.prq')

  ##

##