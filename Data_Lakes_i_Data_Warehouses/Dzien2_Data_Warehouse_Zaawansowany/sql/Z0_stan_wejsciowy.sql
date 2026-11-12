-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z0 - stan wejsciowy Silver z dnia 1
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Te cztery liczby to punkt odniesienia na caly dzien. Kazda tabela Gold ma
-- sie do nich rekoncyliowac. 409 (nie 486) to pozycje po odfiltrowaniu
-- zamowien cancelled w Silver - ten sam JOIN co w L5 dnia 1.
-- =============================================================================

SELECT 'orders' AS tabela, COUNT(*) AS n FROM aurora_jk.silver_orders
UNION ALL SELECT 'order_items', COUNT(*) FROM aurora_jk.silver_order_items
UNION ALL SELECT 'customers',   COUNT(*) FROM aurora_jk.silver_customers
UNION ALL SELECT 'products',    COUNT(*) FROM aurora_jk.silver_products
ORDER BY tabela;

-- Oczekiwane (ORDER BY tabela):
--   customers 80  - silver_customers: jeden wiersz to jeden klient
--   order_items 409 - silver_order_items: jeden wiersz to jedna pozycja paid/shipped
--   orders 170 - silver_orders: jeden wiersz to jedno zamowienie paid/shipped
--   products 25 - silver_products: jeden wiersz to jeden produkt
