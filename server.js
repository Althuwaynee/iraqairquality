const http = require('http');

const server = http.createServer((req, res) => {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end('<h1>Test Server is Working!</h1><p>Now go find your actual project.</p>');
});

server.listen(8082, () => {
    console.log('Server running at http://localhost:8082/');
});
