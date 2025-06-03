BEGIN;
INSERT INTO clientes_em_memoria (id, nome, saldo) VALUES (1, 'Cliente 1', 100.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 1;
END;

BEGIN;
INSERT INTO clientes_em_memoria (id, nome, saldo) VALUES (2, 'Cliente 2', 200.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 2;
END;

BEGIN;
INSERT INTO clientes_em_memoria (id, nome, saldo) VALUES (3, 'Cliente 3', 300.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 2;
END;

BEGIN;
INSERT INTO clientes_em_memoria (id, nome, saldo) VALUES (4, 'Cliente 4', 400.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 3;

BEGIN;
INSERT INTO clientes_em_memoria (id, nome, saldo) VALUES (5, 'Cliente 6', 600.00);
