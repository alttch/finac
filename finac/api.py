from flask import Flask, jsonify, request

from finac import core, ResourceNotFound, RateNotFound, ResourceAlreadyExists
from finac import OverdraftError, OverlimitError

from types import GeneratorType

app = Flask('finac')

app.config["JSON_SORT_KEYS"] = False

key = None


class AccessDenied(Exception):
    pass


@app.route('/jrpc', methods=['POST'])
def jrpc():
    payload = request.json
    response = []
    for req in payload if isinstance(payload, list) else [payload]:
        if req['jsonrpc'] != '2.0':
            raise RuntimeError('Unsupported protocol')
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
            result = getattr(core, req['method'])(**req.get('params', {}))
            if isinstance(result, GeneratorType):
                result = list(result)
            if i is not None:
                resp['result'] = result
        except AccessDenied:
            append_error(-32000, 'Access denied')
        except ResourceNotFound as e:
            append_error(-32001, str(e))
        except RateNotFound as e:
            append_error(-32002, str(e))
        except OverdraftError as e:
            append_error(-32003, str(e))
        except OverlimitError as e:
            append_error(-32004, str(e))
        except ResourceAlreadyExists as e:
            append_error(-32005, str(e))
        except AttributeError:
            append_error(-32601, 'Method not found')
        except TypeError:
            append_error(-32602, 'Invalid params')
        except Exception as e:
            append_error(-32603, str(e))
        if i is not None:
            response.append(resp)
    if response:
        return jsonify(response) if isinstance(payload, list) else response[0]


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
