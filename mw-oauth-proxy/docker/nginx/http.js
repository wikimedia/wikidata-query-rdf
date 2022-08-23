function hello(r) {
    var requestBody;
    try {
        requestBody = r.requestBody
    } catch (err) {
        requestBody = null;
    }
    r.headersOut['Content-Type'] = 'application/json';
    r.return(200, JSON.stringify({
        "method": r.method,
        "hello": "world!",
        "requestBody": requestBody
    }));
}
