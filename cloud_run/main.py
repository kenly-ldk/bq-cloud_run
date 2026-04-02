from flask import Flask, request, jsonify
import os
import time
import threading
import protegrity_developer_python as protegrity
from protegrity_developer_python.utils.discover import discover
from protegrity_developer_python.utils.constants import DATA_ELEMENT_MAPPING
from appython.protector import Protector

app = Flask(__name__)

# Global variables for session caching
cached_session = None
last_session_time = 0
SESSION_CACHE_TTL = 300  # 5 minutes in seconds
cache_lock = threading.Lock()

def _get_cached_session():
    """Retrieve or refresh the cached session in a thread-safe manner."""
    global cached_session, last_session_time
    current_time = time.time()
    
    # Fast check without lock
    if cached_session is None or (current_time - last_session_time) > SESSION_CACHE_TTL:
        with cache_lock:
            # Double check inside lock to avoid race conditions
            if cached_session is None or (current_time - last_session_time) > SESSION_CACHE_TTL:
                try:
                    local_protector = Protector()
                    new_session = local_protector.create_session("superuser")
                    cached_session = new_session
                    last_session_time = current_time
                    print("Successfully created/refreshed cached session.", flush=True)
                except Exception as e:
                    print(f"Failed to refresh cached session: {e}", flush=True)
                    if cached_session is None:
                        raise e  # If we have NO session at all, we must fail
    return cached_session

def invalidate_cache(failed_session):
    """Force invalidate the cache on session failures, only if it matches the failed session."""
    global cached_session, last_session_time
    with cache_lock:
        if cached_session is failed_session:
            cached_session = None
            last_session_time = 0
            print("Forced invalidation of cached session.", flush=True)
        else:
            print("Ignoring invalidation request for a non-matching session.", flush=True)


# Initialize Protegrity low-level SDK session
# We no longer do a synchronous startup session check to avoid consuming rate limits on instance starts.
# Healthy status will be determined on-demand as requests are processed.
session_ok = True

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
    
    # Retrieve session from cache (thread-safe)
    active_session = _get_cached_session()

    if session_ok or active_session:
         for de, items in by_element.items():
              indices = [item[0] for item in items]
              vals = [item[1] for item in items]
              try:
                   if mode == 'tokenize':
                        protected_vals, errors = active_session.protect(vals, de)
                   else:
                        protected_vals, errors = active_session.unprotect(vals, de)
                   for idx, u_val in zip(indices, protected_vals):
                        replies[idx] = u_val
              except Exception as e:
                   print(f"Bulk unprotect failed for {de}: {e}", flush=True)
                   # Force invalidate cache if it looks like a session failure
                   err_str = str(e).lower()
                   if "session" in err_str or "forbidden" in err_str or "unauthorized" in err_str:
                        invalidate_cache(active_session)
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
             for item in items:
                  if isinstance(item, (list, tuple)) and len(item) >= 2:
                       k, v = item[0], item[1]
                  elif isinstance(item, dict):
                       k, v = item.get('key'), item.get('value')
                  else:
                       continue
                       
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
        
        if True:
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
