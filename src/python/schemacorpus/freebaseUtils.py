import string, sys, os, bisect, heapq, random, time, httplib
import urllib, urllib2, simplejson, pickle

loginhost = 'sandbox.freebase.com'           # The Metaweb host
loginservice = '/api/account/login'     # Path to login service
queryhost = 'www.freebase.com'              # The Metaweb host
readservice = '/api/service/mqlread'   # Path to mqlread service

#
# If anything goes wrong when talking to a Metaweb service, we raise MQLError.
#
class MQLError(Exception):
  def __init__(self, value):     # This is the exception constructor method
    self.value = value
  def __str__(self):             # Convert error object to a string
    return repr(self.value)
#
# Submit the specified username and password to the Metaweb login service.
# Return opaque authentication credentials on success. 
# Raise MQLError on failure.
#
def login(username, password):
  # Establish a connection to the server and make a request.
  # Note that we use the low-level httplib library instead of urllib2.
  # This allows us to manage cookies explicitly.
  conn = httplib.HTTPConnection(loginhost)
  conn.request('POST',                   # POST the request
               loginservice,             # The URL path /api/account/login
               # The body of the request: encoded username/password
               urllib.urlencode({'username':username, 'password':password}),
               # This header specifies how the body of the post is encoded.
               {'Content-type': 'application/x-www-form-urlencoded'})

  # Get the response from the server
  print "Waiting for response...."
  response = conn.getresponse()

  print "Received login response status", response.status
  if response.status == 200:  # We get HTTP 200 OK even if login fails
    # Parse response body and raise a MQLError if login failed
    contentlen = int(response.getheader("Content-Length"))
    body = simplejson.loads(response.read(contentlen))
    if not body['code'].startswith('/api/status/ok'):
      error = body['messages'][0]
      raise MQLError('%s: %s' % (error['code'], error['message']))

    # Otherwise return cookies to serve as authentication credentials.
    # The set-cookie header holds one or more cookie specifications,
    # separated by commas. Each specification is a name, an equal
    # sign, a value, and one or more trailing clauses that consist
    # of a semicolon and some metadata.  We don't care about the
    # metadata. We just want to return a comma-separated list of
    # name=value pairs.
    cookies = response.getheader('set-cookie').split(',')
    return ';'.join([c[0:c.index(';')] for c in cookies])
  else:                      # This should never happen
    raise MQLError('HTTP Error: %d %s' % (response.status,response.reason))

#
# Submit the MQL query q and return the result as a Python object.
# If authentication credentials are supplied, use them in a cookie.
# Raises MQLError if the query was invalid. Raises urllib2.HTTPError if
# mqlread returns an HTTP status code other than 200 (which should not happen).
#
def read(q, credentials=None, cursor=False):
  # Put the query in an envelope
  if cursor:
    env = {'qname':{'query':q, 'cursor': cursor}}
  else:
    env = {'qname':{'query':q}}

  # JSON serialize and URL encode the envelope and the query parameter
  args = urllib.urlencode({'queries':simplejson.dumps(env)})
  # Build the URL and create a Request object for it
  url = 'http://%s%s?%s' % (queryhost, readservice, args)
  req = urllib2.Request(url)

  # Send our authentication credentials, if any, as a cookie.
  # The need for mqlread authentication is a temporary restriction.
  if credentials: 
    req.add_header('Cookie', credentials)

  # Now upen the URL and and parse its JSON content
  f = urllib2.urlopen(req)        # Open the URL
  response = simplejson.load(f)   # Parse JSON response to an object
  inner = response['qname']       # Open outer envelope; get inner envelope

  # If anything was wrong with the invocation, mqlread will return an HTTP
  # error, and the code above with raise urllib2.HTTPError.
  # If anything was wrong with the query, we won't get an HTTP error, but
  # will get an error status code in the response envelope.  In this case
  # we raise our own MQLError exception.
  if not inner['code'].startswith('/api/status/ok'):
    error = inner['messages'][0]
    raise MQLError('%s: %s' % (error['code'], error['message']))

  # If there was no error, then just return the result from the envelope
  if cursor:
    return inner['result'], inner["cursor"]
  else:
    return inner['result']

#
# readBatch will use a cursor to grab batches of results.
# It's possible that the overall set is larger than what can fit into
# memory, so the user provides a function that processes each batch
# of results.  That probably means sending them to disk, but who knows?
#
def readBatch(q, processResultsFn, credentials=None, maxResults=-1):
  results, cursorVal = read(q, credentials, True)
  processResultsFn(results)
  totalResults = 0
  while cursorVal:
    results, cursorVal = read(q, credentials, cursorVal)
    processResultsFn(results)
    totalResults += len(results)
    if maxResults >= 0 and totalResults >= maxResults:
      return
                 
