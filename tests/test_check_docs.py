import tempfile
import unittest
from pathlib import Path

from check_docs import check_document, check_translations, example_blocks


class DocumentationChecks(unittest.TestCase):
    def check(self, text, keys=None, extra=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            doc = root / 'guide.md'
            doc.write_text(text)
            if extra:
                (root / 'other.md').write_text(extra)
            return check_document(doc, root, keys)

    def test_valid_relative_heading_and_external_url(self):
        self.assertEqual(self.check('[安装](other.md#安装步骤) [外部](https://example.com)',
                                    extra='# 安装步骤\n'), [])

    def test_missing_file_and_heading(self):
        self.assertTrue(self.check('[missing](absent.md)'))
        self.assertTrue(self.check('[missing](other.md#absent)', extra='# Present\n'))

    def test_link_must_stay_in_repository(self):
        self.assertTrue(self.check('[outside](../outside.md)'))

    def test_configuration_json_and_unknown_key(self):
        self.assertEqual(self.check('```json\n{"http_port": 3012}\n```', {'http_port'}), [])
        self.assertTrue(self.check('```json\n{"typo": 3012}\n```', {'http_port'}))
        self.assertTrue(self.check('```json\n{"http_port": }\n```', {'http_port'}))

    def test_private_workspace_reference(self):
        self.assertTrue(self.check('Source lives at /Users/example/private-project'))


class TranslationChecks(unittest.TestCase):
    def pair_errors(self, english, chinese=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'README.md').write_text(english)
            if chinese is not None:
                (root / 'README.zh-CN.md').write_text(chinese)
            return check_translations(root)

    def test_requires_translation_and_reciprocal_navigation(self):
        self.assertTrue(self.pair_errors('# Docs\n'))
        self.assertTrue(self.pair_errors('# Docs\n', '# 文档\n'))
        self.assertEqual(self.pair_errors('# Docs\n\n[中文](README.zh-CN.md)\n',
                                         '# 文档\n\n[English](README.md)\n'), [])

    def test_detects_command_drift(self):
        english = '# Docs\n\n[中文](README.zh-CN.md)\n\n```sh\nmake build\n```'
        chinese = '# 文档\n\n[English](README.md)\n\n```sh\nmake test\n```'
        self.assertTrue(any('examples differ' in e for e in self.pair_errors(english, chinese)))

    def test_allows_translated_comments_and_json_formatting(self):
        self.assertEqual(example_blocks('```sh\n# Build\nmake build\n```'),
                         example_blocks('```sh\n# 构建\nmake build\n```'))
        self.assertEqual(example_blocks('```json\n{"http_port":3012}\n```'),
                         example_blocks('```json\n{ "http_port": 3012 }\n```'))

    def test_detects_orphan_translation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'extra.zh-CN.md').write_text('# 文档\n')
            self.assertTrue(any('Orphan' in e for e in check_translations(root)))


if __name__ == '__main__':
    unittest.main()
