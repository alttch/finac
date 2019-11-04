#!/usr/bin/env python3

TEST_DB = '/tmp/finac-test-server.db'

# for tests only
from pathlib import Path
import sys
import os
sys.path.insert(0, Path().absolute().parent.as_posix())
import finac, finac.api
try:
    os.unlink(TEST_DB)
except:
    pass
finac.core.rate_cache = None
### 

finac.init(TEST_DB)
finac.api.key = 'secret'
application = finac.api.app

if __name__ == '__main__':
    application.run(host='0.0.0.0', debug=True)
