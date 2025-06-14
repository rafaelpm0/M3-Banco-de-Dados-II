let credentials = {
  "clientId": "cIeEjufAclKhIodqNgAwjYvg",
  "secret": "hGHO+nW19KK-Oj,QerGxXnuzcZD0o4kEM7T4CvGxIETqbIEbl7PSApcp4pMO6Y_M+gwClbJ+XR2zNhbmk21qx-G0.pzp9n8zatdi,psxQHQAHIjK69q4.GuP_D6SsdHR",
  "token": "AstraCS:cIeEjufAclKhIodqNgAwjYvg:ccfd225605dd914d275f7a680c5e011ea28c42fba8f42fefd25371ad4881f14e"
};

import { Client } from "cassandra-driver";

/**
 * Função para criar uma conexão com o banco de dados Cassandra.
 * @returns {Client} - Instância do Cassandra Client conectada ao banco de dados.
 */
function connectToCassandra() {
  const client = new Client({
    cloud: {
      secureConnectBundle: "C:\\Users\\Guilherme\\Desktop\\Faculdade\\Repositorio_De_Listas_e_Trabalhos_CC\\5_Semestre\\Banco_De_Dados\\T3\\banco_mv3_pt1\\DBs\\CassandraDB\\secure-connect-cassandradb.zip",
    },
    credentials: {
      username: credentials.clientId,
      password: credentials.secret,
    },
    keyspace: 'employees' // <-- Adicione esta linha com o nome do seu keyspace
  });
  return client;
}

/**
 * Função para encerrar a conexão com o banco de dados Cassandra.
 * @param {Client} client - Instância do Cassandra Client conectada ao banco de dados.
 */
async function closeCassandraConnection(client) {
  try {
    await client.shutdown();
  } catch (error) {
    console.error('Erro ao encerrar a conexão com o Cassandra:', error.message);
  }
}

export { connectToCassandra, closeCassandraConnection };
