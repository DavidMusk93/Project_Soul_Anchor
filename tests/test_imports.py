import unittest


class TestImports(unittest.TestCase):
    def test_legacy_import_remains_available(self):
        from MemoryManager import MemoryManager  # noqa: PLC0415

        self.assertEqual(MemoryManager.__name__, "MemoryManager")

    def test_package_import_is_available(self):
        from soul_anchor.manager import MemoryManager  # noqa: PLC0415

        self.assertEqual(MemoryManager.__name__, "MemoryManager")


if __name__ == "__main__":
    unittest.main()
