from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from pymongo import MongoClient
from datetime import datetime
from configs import config as config, mongodb as mongo

# Conexão MongoDB
mongo_client = MongoClient(mongo.MONGO_URI)
mongo_db = mongo_client[mongo.DB_NAME]
feedback_collection = mongo_db[config.DATABASE_TABLENAME_WEBSITE_FEEDBACK]

# HTML embutido no código
HTML_CONTENT = '''<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Deixe o seu Feedback</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }
        .container {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 24px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            max-width: 500px;
            width: 100%;
            padding: 40px;
            animation: slideIn 0.6s ease-out;
        }
        @keyframes slideIn {
            from { opacity: 0; transform: translateY(-30px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .header {
            text-align: center;
            margin-bottom: 35px;
        }
        .icon {
            width: 70px;
            height: 70px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 20px;
            box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4);
        }
        .icon svg {
            width: 35px;
            height: 35px;
            fill: white;
        }
        h1 {
            color: #2d3748;
            font-size: 28px;
            margin-bottom: 8px;
            font-weight: 700;
        }
        .subtitle {
            color: #718096;
            font-size: 15px;
        }
        .form-group {
            margin-bottom: 25px;
        }
        label {
            display: block;
            color: #4a5568;
            font-weight: 600;
            margin-bottom: 8px;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        input, textarea {
            width: 100%;
            padding: 14px 16px;
            border: 2px solid #e2e8f0;
            border-radius: 12px;
            font-size: 15px;
            font-family: inherit;
            transition: all 0.3s ease;
            background: #f7fafc;
        }
        input:focus, textarea:focus {
            outline: none;
            border-color: #667eea;
            background: white;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        textarea {
            resize: vertical;
            min-height: 120px;
        }
        .rating-group {
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .rating-group input {
            width: auto;
            flex: 1;
        }
        .rating-value {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 8px 16px;
            border-radius: 8px;
            font-weight: 700;
            min-width: 50px;
            text-align: center;
            font-size: 18px;
        }
        .submit-btn {
            width: 100%;
            padding: 16px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 12px;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 1px;
            box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4);
        }
        .submit-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 12px 28px rgba(102, 126, 234, 0.5);
        }
        .success-message {
            display: none;
            background: #48bb78;
            color: white;
            padding: 16px;
            border-radius: 12px;
            text-align: center;
            margin-top: 20px;
            animation: slideIn 0.4s ease-out;
        }
        .success-message.show {
            display: block;
        }
        .error-message {
            display: none;
            background: #f56565;
            color: white;
            padding: 16px;
            border-radius: 12px;
            text-align: center;
            margin-top: 20px;
        }
        .error-message.show {
            display: block;
        }
        .char-count {
            text-align: right;
            color: #a0aec0;
            font-size: 12px;
            margin-top: 4px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="icon">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">
                    <path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/>
                </svg>
            </div>
            <h1>Deixe o seu Feedback</h1>
            <p class="subtitle">A sua opinião é importante para nós</p>
        </div>
        <form id="feedbackForm">
            <div class="form-group">
                <label for="email">Email</label>
                <input type="email" id="email" placeholder="seu.email@exemplo.com" required>
            </div>
            <div class="form-group">
                <label for="comentarios">Comentários</label>
                <textarea id="comentarios" placeholder="Partilhe a sua experiência..." maxlength="500" required></textarea>
                <div class="char-count"><span id="charCount">0</span>/500 caracteres</div>
            </div>
            <div class="form-group">
                <label for="classificacao">Classificação (1-10)</label>
                <div class="rating-group">
                    <input type="range" id="classificacao" min="1" max="10" value="5" required>
                    <div class="rating-value" id="ratingValue">5</div>
                </div>
            </div>
            <button type="submit" class="submit-btn">Enviar Feedback</button>
        </form>
        <div class="success-message" id="successMessage">✓ Feedback enviado com sucesso!</div>
        <div class="error-message" id="errorMessage">❌ Erro ao enviar.</div>
    </div>
    <script>
        const form = document.getElementById('feedbackForm');
        const classificacaoInput = document.getElementById('classificacao');
        const ratingValue = document.getElementById('ratingValue');
        const comentariosInput = document.getElementById('comentarios');
        const charCount = document.getElementById('charCount');
        const successMessage = document.getElementById('successMessage');
        const errorMessage = document.getElementById('errorMessage');

        classificacaoInput.addEventListener('input', function() {
            ratingValue.textContent = this.value;
        });

        comentariosInput.addEventListener('input', function() {
            charCount.textContent = this.value.length;
        });

        form.addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = {
                email: document.getElementById('email').value,
                comentarios: document.getElementById('comentarios').value,
                classificacao: parseInt(document.getElementById('classificacao').value)
            };

            fetch('/submit', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(formData)
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    successMessage.classList.add('show');
                    errorMessage.classList.remove('show');
                    form.reset();
                    ratingValue.textContent = '5';
                    charCount.textContent = '0';
                    setTimeout(() => successMessage.classList.remove('show'), 5000);
                }
            })
            .catch(error => {
                errorMessage.classList.add('show');
                successMessage.classList.remove('show');
            });
        });
    </script>
</body>
</html>'''

class MyHandler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(HTML_CONTENT.encode('utf-8'))
        else:
            self.send_error(404)
    
    def do_POST(self):
        if self.path == '/submit':
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                # data['datahora'] = datetime.now()             # faço isso na base dados, assume data/hora da base dados p/ uniformizar formato
                data['from'] = "website feedback"               # adiciono pq não vem do formulário/site
                data['extracted'] = False                       # adiciono pq não vem do formulário/site
                result = feedback_collection.insert_one(data)
                
                print(f"✓ Feedback inserido! ID: {result.inserted_id}")
                print(f"  Email: {data['email']}")
                print(f"  Comentário: {data['comentarios']}")
                print(f"  Classificação: {data['classificacao']}/10")
                print(f"  From: {data['from']}")
                print(f"  Extracted: {data['extracted']}")
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = json.dumps({'status': 'success', 'id': str(result.inserted_id)})
                self.wfile.write(response.encode('utf-8'))
                
            except Exception as e:
                print(f"Erro: {e}")
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = json.dumps({'status': 'error', 'message': str(e)})
                self.wfile.write(response.encode('utf-8'))

if __name__ == '__main__':
    PORT = 8000
    server = HTTPServer(('localhost', PORT), MyHandler)
    
    print('=' * 60)
    print(f'Servidor a correr em http://localhost:{PORT}')
    print(f'MongoDB: esd_wine.feedback')
    print('=' * 60)
    print('Pressiona Ctrl+C para parar\n')
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nServidor parado!')