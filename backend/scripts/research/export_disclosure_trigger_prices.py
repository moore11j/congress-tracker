"""Read-only coverage supplement for both frozen disclosure cohorts."""
import base64,json,zlib
from sqlalchemy import text,bindparam
from app.db import SessionLocal
SYMBOLS = ['ACOG', 'AFCG', 'AGCO', 'AIRS', 'ALKT', 'AMR', 'ANIX', 'APCX', 'ARTV', 'ASA', 'ATLO', 'AWRE', 'AXR', 'BCDA', 'BETR', 'BGDE', 'BLND', 'BOLD', 'BWFG', 'BZUN', 'CLIK', 'COE', 'CRT', 'CSBB', 'CTTH', 'CUEN', 'CZR', 'DGICA', 'DK', 'DLHC', 'DLPN', 'EHAB', 'ENOV', 'ENR', 'FOUR', 'FTCI', 'GAIA', 'GF', 'GOOD', 'GOTU', 'GPUS', 'GRX', 'GTE', 'GWRS', 'HDSN', 'HFRO', 'HKHC', 'IRIX', 'JCTC', 'KDOZF', 'KRNY', 'KWY', 'LGHL', 'LILA', 'LOGC', 'MDRR', 'MHH', 'MKTW', 'MLYS', 'MNTR', 'MOGU', 'MTDR', 'MXF', 'NE', 'NFJ', 'NFRX', 'NMM', 'NONE', 'NRDY', 'NWFL', 'NXDT', 'NYC', 'OPFI', 'PATK', 'PAX', 'PBF', 'PFBX', 'PMT', 'PODC', 'PTIX', 'QVCG', 'RCG', 'REYN', 'RGCO', 'RIG', 'RLYB', 'RSG', 'SBMT', 'SLGL', 'SNES', 'SON', 'SONO', 'SPY', 'SRTS', 'STEX', 'STIM', 'STRR', 'SUJA', 'SWZ', 'TCBI', 'TOFB', 'TOI', 'TPL', 'TSM', 'TXO', 'UUU', 'VANI', 'VENU', 'VIRC', 'VMAR', 'WGS', 'WHF', 'WRB', 'WSC', 'YQ', 'ZBIO']
with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='60s'")
    query=text("SELECT symbol,date,raw_close,adjusted_close,open_price,volume,price_source,adjustment_status,split_coefficient FROM price_cache WHERE symbol IN :symbols AND date BETWEEN '2023-01-01' AND '2026-09-11' AND raw_close>0 AND price_source IN ('fmp:historical-price-eod/full+corporate_actions','massive:grouped-daily-adjusted') ORDER BY symbol,date").bindparams(bindparam('symbols',expanding=True)).execution_options(stream_results=True,yield_per=2000)
    rows=[list(r) for r in db.execute(query,{'symbols':SYMBOLS})]
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(json.dumps({'symbols':SYMBOLS,'rows':rows},default=str,separators=(',',':')).encode())).decode(),flush=True)
    db.rollback()
