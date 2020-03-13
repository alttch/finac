from flask import Flask, jsonify, request, Response

from finac import core, ResourceNotFound, RateNotFound, ResourceAlreadyExists
from finac import OverdraftError, OverlimitError
from finac.core import get_db, logger

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


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
