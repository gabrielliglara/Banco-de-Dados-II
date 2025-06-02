CREATE DATABASE BANCO_REDO;
/c BANCO_REDO


CREATE UNLOGGED TABLE clientes_em_memoria (
  id SERIAL PRIMARY KEY,
  nome TEXT,
  saldo NUMERIC
);

CREATE TABLE log (operacao TEXT, id_cliente INT, nome TEXT, saldo NUMERIC, status TEXT);

CREATE FUNCTION LOG_FUNCTION() 
RETURNS trigger AS $$
    BEGIN
        
        RETURN;
    END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER TG_LOG
BEFORE INSERT OR UPDATE OR DELETE
ON clientes_em_memoria
FOR each row EXECUTE PROCEDURE
LOG_FUNCTION();

BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 1', 100.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 1;
END;

BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 2', 200.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 2;
END;

BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 3', 300.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 2;
END;

BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 4', 400.00);
UPDATE clientes_em_memoria SET saldo = saldo + 50 WHERE id = 3;

BEGIN;
INSERT INTO clientes_em_memoria (nome, saldo) VALUES ('Cliente 6', 600.00);
