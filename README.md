# Banco-de-Dados-II

Objetivo do trabalho: 
implementar o mecanismo de log Redo usando o SGBD

Funcionamento: 
Uma tabela em memória deverá ser criada para esta atividade. Algumas operações de insert e update serão simuladas. Vc deverá criar uma tabela (normal) para salvar as operações de log. Após a queda dos sistema, o arquivo de log deverá ser processado para materializar as operações a partir do mecanismo de REDO. 

Passos: 
1- Criar uma tabela em memória (CREATE UNLOGGED TABLE);
2- Inserir tuplas na tabela a partir de transações (insert, update, delete);
3- Salvar as operações do passo 2 em uma tabela de log;
4- Derrubar o SGBD;
5- Usar o arquivo de log para reconstruir as operações que comitaram e restabelecer os dados em memória; 

Arquivo de entrada: 
Script com as criação das tabelas(tabela em memória e o log) e as transações; 
