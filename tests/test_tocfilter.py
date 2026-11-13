#!/usr/bin/env python3
"""Unit tests for the TOC filter (plain tables + indexes/constraints only)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from tocfilter import filter_toc  # noqa: E402

TOC = """\
; Archive created at 2026-11-13 10:00:00 UTC
;     dbname: sample_normal
;     TOC Entries: 30
;     Compression: 6
;     Dump Version: 1.15-0
;     Format: CUSTOM
;
; Selected TOC Entries:
;
3103; 0 0 ENCODING - ENCODING
3104; 0 0 STDSTRINGS - STDSTRINGS
8; 2615 2200 SCHEMA - public postgres
9; 2615 3300 SCHEMA - analytics postgres
3210; 0 0 EXTENSION - plpgsql
220; 1259 16391 TABLE public customers postgres
221; 1259 16397 TABLE public orders postgres
3300; 0 16391 TABLE DATA public customers postgres
3301; 0 16397 TABLE DATA public orders postgres
222; 1259 16390 SEQUENCE public customers_id_seq postgres
3400; 0 0 SEQUENCE SET public customers_id_seq postgres
223; 0 0 SEQUENCE OWNED BY public customers_id_seq postgres
2900; 2604 16392 DEFAULT public customers id postgres
3200; 2606 16410 CONSTRAINT public customers customers_pkey postgres
3201; 2606 16412 CONSTRAINT public customers customers_email_key postgres
3202; 2606 16420 FK CONSTRAINT public orders orders_customer_id_fkey postgres
3100; 1259 16415 INDEX public idx_orders_customer postgres
3225; 0 0 COMMENT - SCHEMA public postgres
4100; 1255 16430 FUNCTION public do_stuff() postgres
4200; 1259 16440 VIEW public v_orders postgres
"""


class TestTocFilter(unittest.TestCase):
    def setUp(self):
        self.res = filter_toc(TOC)
        self.active = [l for l in self.res.lines if l.strip() and not l.lstrip().startswith(";")]
        self.commented = [l for l in self.res.lines if l.lstrip().startswith(";")]

    def test_pseudo_entries_kept(self):
        self.assertTrue(any("ENCODING" in l for l in self.active))
        self.assertTrue(any("STDSTRINGS" in l for l in self.active))

    def test_public_schema_commented_but_not_counted(self):
        self.assertFalse(any("SCHEMA - public" in l for l in self.active))
        self.assertNotIn("SCHEMA", self.res.skipped)

    def test_non_public_schema_kept(self):
        self.assertTrue(any("SCHEMA - analytics" in l for l in self.active))

    def test_supported_objects_kept(self):
        for needle in (
            "TABLE public customers",
            "TABLE public orders",
            "TABLE DATA public customers",
            "TABLE DATA public orders",
            "SEQUENCE public customers_id_seq",
            "SEQUENCE SET",
            "SEQUENCE OWNED BY",
            "DEFAULT public customers id",
            "CONSTRAINT public customers customers_pkey",
            "CONSTRAINT public customers customers_email_key",
            "FK CONSTRAINT public orders",
            "INDEX public idx_orders_customer",
        ):
            self.assertTrue(any(needle in l for l in self.active), needle)

    def test_unsupported_objects_skipped(self):
        self.assertEqual(self.res.skipped.get("EXTENSION"), 1)
        self.assertEqual(self.res.skipped.get("COMMENT"), 1)
        self.assertEqual(self.res.skipped.get("FUNCTION"), 1)
        self.assertEqual(self.res.skipped.get("VIEW"), 1)
        self.assertFalse(any(" EXTENSION " in l for l in self.active))
        self.assertFalse(any(" FUNCTION " in l for l in self.active))
        self.assertFalse(any(" VIEW " in l for l in self.active))

    def test_unknown_type_is_skipped_not_restored(self):
        res = filter_toc("1; 2 3 FROBNICATE public x postgres\n")
        self.assertEqual(res.skipped.get("FROBNICATE"), 1)
        self.assertEqual(res.kept, 0)

    def test_multiword_types(self):
        res = filter_toc(
            "1; 2 3 MATERIALIZED VIEW DATA public mv postgres\n"
            "2; 2 3 DEFAULT ACL - public postgres\n"
        )
        self.assertEqual(res.skipped.get("MATERIALIZED VIEW DATA"), 1)
        self.assertEqual(res.skipped.get("DEFAULT ACL"), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
