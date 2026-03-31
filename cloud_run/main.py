from flask import Flask, request, jsonify
import os
import protegrity_developer_python as protegrity
from protegrity_developer_python.utils.discover import discover
from protegrity_developer_python.utils.constants import DATA_ELEMENT_MAPPING
from appython.protector import Protector

app = Flask(__name__)

# Initialize Protegrity low-level SDK session
try:
    protector = Protector()
    pty_session = protector.create_session("superuser") 
    print("Protegrity direct session initialized successfully.")
    session_ok = True
except Exception as e:
    print(f"Error initializing Protegrity direct session: {e}")
    session_ok = False

# Initialize Protegrity high-level (for discovery setup if needed)
try:
    base_url = os.environ.get("PROTEGRITY_SERVER_URL", "http://localhost:8050")
    endpoint_url = base_url
    if not base_url.endswith("/classify") and not base_url.endswith("/v1.1"):
         if base_url.endswith(":8050") or base_url.endswith("localhost:8050"):
              endpoint_url = f"{base_url}/pty/data-discovery/v1.1/classify"
    
    named_entity_map = {
        "EMAIL_ADDRESS": "EMAIL_ADDRESS",
        "SOCIAL_SECURITY_ID": "SOCIAL_SECURITY_ID",
        "DOB": "DOB",
        "DATETIME": "DATETIME",
        "PERSON": "PERSON"
    }

    protegrity.configure(
        endpoint_url=endpoint_url,
        named_entity_map=named_entity_map,
        enable_logging=True,
        log_level="info"
    )
    print("Protegrity high-level wrapper configured successfully.")
except Exception as e:
    print(f"Error configuring Protegrity high-level wrapper: {e}")

def _process_bulk_pii(calls, data_element_from_context, session_ok, mode='detokenize'):
    # Group calls by data element
    by_element = {}
    for i, call in enumerate(calls):
        if not call or len(call) == 0:
             continue
        val = call[0]
        if val is None:
             continue
        
        de = call[1] if len(call) > 1 else data_element_from_context or "email"
        if de not in by_element:
             by_element[de] = []
        by_element[de].append((i, val))

    replies = [None] * len(calls)
    
    if session_ok:
         for de, items in by_element.items():
              indices = [item[0] for item in items]
              vals = [item[1] for item in items]
              try:
                   if mode == 'tokenize':
                        protected_vals, errors = pty_session.protect(vals, de)
                   else:
                        protected_vals, errors = pty_session.unprotect(vals, de)
                   for idx, u_val in zip(indices, protected_vals):
                        replies[idx] = u_val
              except Exception as e:
                   print(f"Bulk unprotect failed for {de}: {e}")
                   for idx in indices:
                        replies[idx] = f"error_{de}"
    else:
         for i in range(len(calls)):
              replies[i] = "no_session"

    return replies

@app.route('/', methods=['POST'])
def process_pii():
    try:
        req = request.get_json()
        calls = req.get('calls', []) if req else []
        
        user_context = req.get('userDefinedContext', []) or req.get('user_defined_context', [])
        data_element_from_context = None
        mode = None
        if user_context:
             items = user_context.items() if isinstance(user_context, dict) else user_context
             for k, v in items:
                  if k == 'data_element':
                       data_element_from_context = v
                  elif k == 'mode':
                       mode = v

        if mode == 'noop':
             replies = []
             for call in calls:
                  if call and len(call) > 0:
                       replies.append(f"noop_{call[0]}")
                  else:
                       replies.append(None)
             return jsonify({"replies": replies})

        replies = _process_bulk_pii(calls, data_element_from_context, session_ok, mode=mode)
        return jsonify({"replies": replies})
    except Exception as e:
        return jsonify({"errorMessage": str(e)}), 400



@app.route('/tokenize_bulk', methods=['POST'])
def tokenize_pii_bulk():
    global pty_session
    try:
        pty_session = protector.create_session("superuser")
    except Exception as e:
        print(f"Failed to create session in /tokenize_bulk: {e}")
        # continue and hope old session works

    try:
        req = request.get_json()
        calls = req.get('calls', [])
        
        user_context = req.get('userDefinedContext', []) or req.get('user_defined_context', [])
        data_element_from_context = None
        if user_context:
            if isinstance(user_context, dict):
                 data_element_from_context = user_context.get('data_element')
            elif isinstance(user_context, list):
                 for k, v in user_context:
                      if k == 'data_element':
                           data_element_from_context = v
                           break

        # Group calls by data element
        by_element = {}
        for i, call in enumerate(calls):
            if not call or len(call) == 0:
                 continue
            val = call[0]
            if val is None:
                 continue
            
            de = call[1] if len(call) > 1 else data_element_from_context or "email"
            if de not in by_element:
                 by_element[de] = []
            by_element[de].append((i, val))

        replies = [None] * len(calls)
        
        if True: # session_ok check omitted for simplicity, assume ok if create_session didn't raise
             for de, items in by_element.items():
                 indices = [item[0] for item in items]
                 vals = [item[1] for item in items]
                 try:
                      protected_vals, errors = pty_session.protect(vals, de)
                      for idx, p_val in zip(indices, protected_vals):
                           replies[idx] = p_val
                 except Exception as e:
                      print(f"Bulk protect failed for {de}: {e}")
                      for idx in indices:
                           replies[idx] = f"error_{de}"

        return jsonify({"replies": replies})
    except Exception as e:
        return jsonify({"errorMessage": str(e)}), 400

@app.route('/noop', methods=['POST'])
def noop_pii():
    try:
        req = request.get_json()
        calls = req.get('calls', [])
        
        replies = []
        for call in calls:
            if call and len(call) > 0:
                val = call[0]
                if val is not None:
                     replies.append(f"noop_{val}")
                else:
                     replies.append(None)
            else:
                replies.append(None)

        return jsonify({"replies": replies})
    except Exception as e:
        return jsonify({"errorMessage": str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
