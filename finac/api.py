import time
import datetime

from flask import Flask, jsonify, request, Response

from finac import core, ResourceNotFound, RateNotFound, ResourceAlreadyExists
from finac import OverdraftError, OverlimitError
from finac.core import get_db, logger, exec_query, spawn

from types import GeneratorType

app = Flask('finac')

app.config["JSON_SORT_KEYS"] = False

key = None

real_ip_header = None


def get_real_ip():
    return request.headers.get(real_ip_header, request.remote_addr) if \
        real_ip_header else request.remote_addr


class AccessDenied(Exception):
    pass


@app.route('/ping')
def ping():
    get_db()
    return jsonify({'ok': True})


def _check_x_auth_key(log_from):
    k = request.headers.get('X-Auth-Key')
    if key is not None:
        if k is None:
            logger.error(f'{log_from} access denied')
            return Response('API Key not specified', status=401)
        elif k != key:
            logger.error(f'{log_from} access denied')
            return Response('Invalid API key', status=403)
    return True


@app.route('/query', methods=['POST'])
def query_post():
    log_from = 'FINAC QUERY API request from ' + get_real_ip()
    result = _check_x_auth_key(log_from)
    if result is not True:
        return result
    if isinstance(request.json, list):
        futures = [
            spawn(query,
                  q,
                  _return_raw=True,
                  _check_perm=False,
                  log_from=log_from,
                  _time_ms=request.args.get('time_ms') == '1')
            for q in request.json
            if q
        ]
        return jsonify([f.result() for f in futures])
    else:
        return Response('Input JSON should be list of queries', status=400)


@app.route('/query', methods=['GET'])
def query(q=None,
          _return_raw=False,
          _check_perm=True,
          log_from=None,
          _time_ms=None):

    def _response(text, status):
        if _return_raw:
            return {'error': text}
        else:
            return Response(text, status=status)

    if log_from is None:
        log_from = 'FINAC QUERY API request from ' + get_real_ip()
    if _check_perm:
        result = _check_x_auth_key(log_from)
        if result is not True:
            return result

    if q is None:
        q = request.args.get('q')
    if isinstance(q, list):
        need_ts = q[1]
        q = q[0]
    else:
        need_ts = False
    logger.info(f'{log_from}, query: \'{q}\'')
    if q is None:
        return _response('q param is required', status=400)
    try:
        if _time_ms is None:
            _time_ms = request.args.get('time_ms') == '1'
        t_start = time.time()
        result = list(exec_query(q, _time_ms=_time_ms))
        t_spent = time.time() - t_start
        if _time_ms:
            if need_ts:
                gres = {}
            else:
                gres = {'columns': [], 'rows': [], 'type': 'table'}
            if result:
                timecols = []
                cols = [c for c in result[0]]
                for c in cols:
                    if not need_ts:
                        col = {'text': c}
                    if isinstance(result[0][c], datetime.datetime):
                        if not need_ts:
                            col['type'] = 'time'
                            if c in ('date', 'time', 'created'):
                                col['sort'] = True
                                col['desc'] = True
                        timecols.append(c)
                    elif not need_ts and (isinstance(result[0][c], int) or
                                          isinstance(result[0][c], float)):
                        col['type'] = 'number'
                    if not need_ts:
                        gres['columns'].append(col)
                if need_ts:
                    if len(timecols) > 1 or (timecols and len(result[0]) > 2):
                        return _response('Unsupported time series query',
                                         status=405)
                    else:
                        if timecols:
                            tc_name = timecols[0]
                            for r in result[0]:
                                if r != tc_name:
                                    gres['target'] = r
                                    dc_name = r
                                    break
                        else:
                            tc_name = None
                            dc_name = list(result[0])[0]
                            gres['target'] = dc_name
                            t = datetime.datetime.now().timestamp() * 1000
                        dp = []
                        for r in result:
                            dp.append([
                                r[dc_name], t if tc_name is None else
                                r[tc_name].timestamp() * 1000
                            ])
                        gres['datapoints'] = dp
                else:
                    for r in result:
                        gres['rows'].append([
                            r[c].timestamp() * 1000 if c in timecols else r[c]
                            for c in cols
                        ])
            return gres if _return_raw else jsonify(gres)
        else:
            result = {
                'ok': True,
                'result': result,
                'rows': len(result),
                'time': t_start
            }
            return result if _return_raw else jsonify(result)
    except (LookupError, ResourceNotFound, RateNotFound) as e:
        return _response('Lookup error ' + str(e), status=404)
    except (ResourceAlreadyExists, OverdraftError, OverlimitError) as e:
        return _response('Already exists ' + str(e), status=409)
    except (TypeError, ValueError) as e:
        return _response(str(e), status=400)
    except Exception as e:
        return _response(str(e), status=500)


@app.route('/jrpc', methods=['POST'])
def jrpc():
    payload = request.json
    response = []
    for req in payload if isinstance(payload, list) else [payload]:
        log_from = 'FINAC API request from ' + get_real_ip()

        if not req or req.get('jsonrpc') != '2.0':
            logger.warning(f'{log_from} unsupported protocol')
            return Response('Unsupported protocol', status=405)
        i = req.get('id')
        if i is not None:
            resp = {'jsonrpc': '2.0', 'id': i}

        def append_error(code, message=''):
            if i is not None:
                resp['error'] = {'code': code, 'message': message}

        try:
            params = req.get('params', {})
            if key is not None and key != params.get('_k'):
                raise AccessDenied
            if '_k' in params:
                del params['_k']
            logger.info(f'{log_from} {req["method"]}')
            logger.debug(req.get('params'))
            result = getattr(core, req['method'])(**req.get('params', {}))
            if isinstance(result, GeneratorType):
                result = list(result)
            if i is not None:
                resp['result'] = result
        except AccessDenied:
            logger.error(f'{log_from} access denied')
            append_error(-32000, 'Access denied')
        except ResourceNotFound as e:
            logger.info(f'{log_from} resource not found')
            append_error(-32001, str(e))
        except RateNotFound as e:
            logger.info(f'{log_from} rate not found')
            append_error(-32002, str(e))
        except OverdraftError as e:
            logger.info(f'{log_from} overdraft error')
            append_error(-32003, str(e))
        except OverlimitError as e:
            logger.info(f'{log_from} overlimit error')
            append_error(-32004, str(e))
        except ResourceAlreadyExists as e:
            logger.info(f'{log_from} resource already exists')
            append_error(-32005, str(e))
        except AttributeError:
            logger.warning(f'{log_from} method not found')
            append_error(-32601, 'Method not found')
        except TypeError:
            logger.warning(f'{log_from} invalid params')
            append_error(-32602, 'Invalid params')
        except ValueError:
            logger.warning(f'{log_from} invalid value')
            append_error(-32603, 'Invalid value')
        except Exception as e:
            logger.warning(f'{log_from} unknown error')
            append_error(-32699, str(e))
        if i is not None:
            response.append(resp)
    if response:
        return jsonify(response) if isinstance(payload, list) else response[0]
    else:
        return Response(status=204)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
