import unittest

from .server import slugify


class SlugifyTests(unittest.TestCase):
    def test_slugify_umlauts(self):
        slug, warning = slugify("Fünf große Äpfel & Öl", 80)
        self.assertEqual(slug, "fuenf-grosse-aepfel-und-oel")
        self.assertIsNone(warning)

    def test_slugify_empty(self):
        slug, warning = slugify("!!!", 80)
        self.assertTrue(slug.startswith("post-"))
        self.assertIsNotNone(warning)


if __name__ == "__main__":
    unittest.main()
