/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbtoolotavio;

import java.sql.*;
import java.util.Arrays;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 *
 * @author otavio
 */
public class DBExportToCSVTool {
    
    public static void main(String[] args) throws Exception {
        
        // Tratamento simples das entradas.
        if(args.length < 4 || args.length > 5) {
            System.out.println("Ferramenta de linha de comando para exportar tabelas em formato CSV");
            System.out.println("By: Otávio Baziewicz Filho - UTFPR\n");
            System.out.println("Uso: java dbtool <SERVIDOR> <USUARIO> <SENHA> <BANCO-DE-DADOS> <TABELA>\n");
            System.out.println("Argumentos:\n"
                    + "\t<SERVIDOR>\t\t- ex: 127.0.0.1\n"
                    + "\t<USUARIO>\t\t- ex: root\n"
                    + "\t<SENHA>\t\t\t- ex: 1234\n"
                    + "\t<BANCO-DE-DADOS>\t- ex: university\n"
                    + "\t<TABELA>\t\t- ex: instructor (Se esse parâmetro não for informado, todas as tabelas serão exportadas)\n");
            System.out.println("Formato de saída: <BANCO-DE-DADOS>-<TABELA>.csv");
            return;
        }
        
        String servidor = args[0];
        String usuario = args[1];
        String senha = args[2];
        String bancoDeDados = args[3];
                        
        // Carregamento do driver do MySQL.
        Class.forName("com.mysql.cj.jdbc.Driver");
        
        try {
            // Inicialização a conexão com o servidor.
            Connection con = DriverManager.getConnection("jdbc:mysql://" + servidor + "/" + bancoDeDados, usuario, senha);            
            
            // Criação de um array contendo todas as tabelas que serão exportadas. Caso a informação não seja passada por argumento, todas as tabelas são buscadas. 
            String[] tabelas = {};
            if(args.length == 5) {
                tabelas = Arrays.copyOf(tabelas, tabelas.length + 1);
                tabelas[tabelas.length - 1] = args[4];
            } else {
                DatabaseMetaData md = con.getMetaData();
                ResultSet rs = md.getTables(con.getCatalog(), null, "%", new String [] {"TABLE"});
                while(rs.next()) {
                    tabelas = Arrays.copyOf(tabelas, tabelas.length + 1);
                    tabelas[tabelas.length - 1] = rs.getString(3);
                }
            }
            
            // Criar um statement
            Statement stmt = con.createStatement();
            
            for (String tabela : tabelas) {
                // Execução da query que recolhe todos os dados
                ResultSet rs = stmt.executeQuery("select * from " + tabela);
                ResultSetMetaData rsmd = rs.getMetaData();
                
                // Leitura das colunas da tabela
                String[] colunas = {};
                for(int i=1; i<=rsmd.getColumnCount(); i++) {
                    colunas = Arrays.copyOf(colunas, colunas.length + 1);
                    colunas[colunas.length - 1] = rsmd.getColumnName(i);
                }
                
                // Criação do arquivo de saída
                try (PrintWriter pw = new PrintWriter(new File(bancoDeDados + "-" + tabela + ".csv"))) {
                    
                    // Escrita do cabeçalho do arquivo
                    StringBuilder sb = new StringBuilder();
                    for(String coluna : colunas) {
                        sb.append(coluna).append(",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append("\n");
                    pw.write(sb.toString());
                    
                    // Escrita das linhas do arquivo
                    while(rs.next()) {
                        StringBuilder csb = new StringBuilder();
                        for(String coluna : colunas) {
                            csb.append(rs.getString(coluna)).append(",");
                        }
                        csb.deleteCharAt(csb.length() - 1);  
                        csb.append("\n");
                        pw.write(csb.toString());
                    }
                    
                  } catch (FileNotFoundException e) {
                      throw e;
                  }
            }
            
            // Fechamento da conexão com o banco de dados.
            con.close();
            
        } catch (FileNotFoundException | SQLException e) {
            System.out.println("ERRO - " + e.getMessage());
        }
    }
    
}