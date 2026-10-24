import functools
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError

spec = importlib.util.spec_from_file_location('frontend_assets', Path(__file__).with_name('check_frontend_assets.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


class FrontendAssetsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'js/common').mkdir(parents=True)
        (self.root / 'css').mkdir()
        (self.root / 'fonts').mkdir()
        self.write('index.html', '<script src="js/external.js"></script><link rel="stylesheet" href="css/site.css">')
        self.write('js/external.js', 'window.ready=true;')
        self.write('js/home-worker.js', "importScripts('common/tools.js');")
        self.write('js/common/tools.js', 'var tools={};')
        self.write('css/site.css', "@font-face {src:url('../fonts/test.woff2?v=1#font')} body{background:url(data:image/png;base64,AA)}")
        self.write('fonts/test.woff2', 'font')
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=str(self.root)))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def write(self, name, content):
        (self.root / name).write_text(content)

    def test_checks_html_worker_imports_and_css_fonts(self):
        self.assertEqual(module.check_frontend_assets(self.url), 6)

    def test_rejects_missing_worker_dependency(self):
        (self.root / 'js/common/tools.js').unlink()
        with self.assertRaises(HTTPError):
            module.check_frontend_assets(self.url)

    def test_rejects_missing_html_dependency(self):
        (self.root / 'js/external.js').unlink()
        with self.assertRaises(HTTPError):
            module.check_frontend_assets(self.url)

    def test_rejects_html_fallback_for_script(self):
        self.write('index.html', '<script src="js/missing.js"></script>')
        # A misconfigured static server can return an HTML fallback with 200.
        class Fallback(QuietHandler):
            def guess_type(self, path):
                return 'text/html' if path.endswith('external.js') else super().guess_type(path)
        self.server.RequestHandlerClass = functools.partial(Fallback, directory=str(self.root))
        self.write('index.html', '<script src="js/external.js"></script>')
        with self.assertRaisesRegex(ValueError, 'Expected JavaScript'):
            module.check_frontend_assets(self.url)


if __name__ == '__main__':
    unittest.main()
