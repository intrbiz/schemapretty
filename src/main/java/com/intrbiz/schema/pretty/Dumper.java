package com.intrbiz.schema.pretty;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class Dumper implements AutoCloseable
{    
    private String url;
    
    private String username;
    
    private String password;
    
    private Connection connection;
    
    public Dumper(String url, String username, String password) throws SQLException
    {
        this.url = url;
        this.username = username;
        this.password = password;
        this.connection = DriverManager.getConnection(this.url, this.username, this.password);
    }
    
    public void dump(File outDir) throws SQLException, IOException
    {
        outDir.mkdirs();
        // list the schemas
        try (PreparedStatement stmt = this.connection.prepareStatement("SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname <> 'public' AND nspname <> 'information_schema' AND nspname !~ '^pg_';"))
        {
            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    String schemaName  = rs.getString(1);
                    String schemaOwner = rs.getString(2);
                    this.dumpSchema(schemaName, schemaOwner, outDir);
                }
            }
        }
    }
    
    private void dumpSchema(String name, String owner, File outDir) throws SQLException, IOException
    {
        File schemaDir    = new File(outDir, name);
        File tablesDir    = new File(schemaDir, "tables");
        File typesDir     = new File(schemaDir, "types");
        File functionsDir = new File(schemaDir, "functions");
        // create dirs
        schemaDir.mkdirs();
        tablesDir.mkdirs();
        typesDir.mkdirs();
        functionsDir.mkdirs();
        // dump the schema
        // create schema
        this.writeCreateSchema(name, owner, new File(schemaDir, "create.sql"));
        // tables
        List<File> tables    = this.dumpTables(name, tablesDir);
        List<File> types     = this.dumpTypes(name, typesDir);
        List<File> functions = this.dumpFunctions(name, functionsDir);
        // build script
        this.writeSchemaBuildScript(name, functions, tables, types, new File(schemaDir, "create.sh"));
        this.writeBuildAllScript(new File(outDir, "create_all.sh"));
        this.writeBuildUtilScript(new File(outDir, "util.sh"));
    }
    
    private void writeSchemaBuildScript(String name, List<File> functions, List<File> tables, List<File> types, File to) throws IOException
    {
        try (Writer out = new BufferedWriter(new FileWriter(to)))
        {
            out.write("#!/bin/sh\n");
            out.write("source ../util.sh\n");
            out.write("# Create schema, executing this script will output the SQL schema to stdout\n\n");
            out.write("begin\n");
            out.write("cat ./create.sql\n\n");
            out.write("# Tables\n");
            for (File table : tables)
            {
                out.write("cat \"./tables/" + table.getName() + "\"\n");    
            }
            out.write("\n");
            out.write("# Types\n");
            for (File type : types)
            {
                out.write("cat \"./types/" + type.getName() + "\"\n");    
            }
            out.write("\n");
            out.write("# Functions\n");
            for (File function : functions)
            {
                out.write("cat \"./functions/" + function.getName() + "\"\n");
            }
            out.write("\n");
            out.write("commit\n");
        }
    }
    
    private void writeBuildAllScript(File to) throws IOException
    {
        try (Writer out = new BufferedWriter(new FileWriter(to)))
        {
            out.write("#!/bin/sh\n");
            out.write("echo \"BEGIN;\"\n");
            out.write("for x in */create.sh ; do\n");
            out.write("  pushd `dirname $x` > /dev/null\n");
            out.write("  IN_TRANS=yes sh ../$x\n");
            out.write("  popd > /dev/null\n");
            out.write("done\n");
            out.write("echo \"COMMIT;\"\n");
        }
    }
    
    private void writeBuildUtilScript(File to) throws IOException
    {
        try (Writer out = new BufferedWriter(new FileWriter(to)))
        {
            out.write("#!/bin/sh\n");
            out.write("\n");
            out.write("function begin {\n");
            out.write("  if [ \"$IN_TRANS\" != \"yes\" ]; then\n");
            out.write("    echo \"BEGIN;\"\n");
            out.write("  fi\n");
            out.write("}\n");
            out.write("\n");
            out.write("function commit {\n");
            out.write("  if [ \"$IN_TRANS\" != \"yes\" ]; then\n");
            out.write("    echo \"COMMIT;\"\n");
            out.write("  fi\n");
            out.write("}\n");
            out.write("\n");
        }
    }
    
    private void writeCreateSchema(String name, String owner, File to) throws SQLException, IOException
    {
        try (Writer fw = new BufferedWriter(new FileWriter(to)))
        {
            fw.write("CREATE SCHEMA IF NOT EXISTS" + name + " AUTHORIZATION " + owner + ";\n\n");
        }
    }
    
    public List<File> dumpTables(String schema, File outDir) throws SQLException, IOException
    {
        List<File> files = new LinkedList<File>();
        try (PreparedStatement stmt = this.connection.prepareStatement("SELECT c.relname, pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) WHERE n.nspname = ? AND c.relkind = 'r' AND c.relpersistence = 'p'"))
        {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    String name = rs.getString(1);
                    String owner = rs.getString(2);
                    // dump the table
                    File tableFile = new File (outDir, name + ".sql");
                    files.add(tableFile);
                    this.dumpTable(schema, name, owner, tableFile);
                }
            }
        }
        return files;
    }
    
    public void dumpTable(String schema, String table, String owner, File to) throws SQLException, IOException
    {
        try (Writer writer = new BufferedWriter(new FileWriter(to)))
        {
            writer.write("CREATE TABLE IF NOT EXISTS \"" + schema + "\".\"" + table + "\" (\n");
            boolean ns = false;
            // get the columns
            try (PreparedStatement stmt = this.connection.prepareStatement("SELECT a.attnum, quote_ident(a.attname), format_type(a.atttypid, a.atttypmod) FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) JOIN pg_attribute a ON (a.attrelid = c.oid) WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 ORDER BY a.attnum"))
            {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        String name = rs.getString(2);
                        String type = rs.getString(3);
                        if (ns) writer.write(",\n");
                        writer.write("  " + name + " " + type.toUpperCase());
                        ns = true;
                    }
                }
            }
            // constraints
            try (PreparedStatement stmt = this.connection.prepareStatement("SELECT quote_ident(c.conname), pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_class r ON (c.conrelid = r.oid) JOIN pg_namespace n ON (r.relnamespace = n.oid) WHERE n.nspname = ? AND r.relname = ? ORDER BY c.contype DESC, c.conname"))
            {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        if (ns) writer.write(",\n");
                        writer.write("  CONSTRAINT " + rs.getString(1) +  " "); 
                        writer.write(rs.getString(2));
                    }
                }
            }
            // end table
            writer.write("\n)");
            // inherits
            try (PreparedStatement stmt = this.connection.prepareStatement("SELECT quote_ident(n2.nspname), quote_ident(r2.relname) FROM pg_inherits i JOIN pg_class r1 ON (i.inhrelid = r1.oid) JOIN pg_namespace n1 ON (r1.relnamespace = n1.oid) JOIN pg_class r2 ON (i.inhparent = r2.oid) JOIN pg_namespace n2 ON (r2.relnamespace = n2.oid) WHERE n1.nspname = ? AND r1.relname = ?"))
            {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        writer.write("\nINHERITS (" + rs.getString(1) + "." + rs.getString(2) + ")");
                    }
                }
            }
            writer.write(";\n\n");
            writer.write("ALTER TABLE \"" + schema + "\".\"" + table + "\" OWNER TO " + owner + ";\n\n");
            // indexes
            try (PreparedStatement stmt = this.connection.prepareStatement("SELECT n2.nspname, c2.relname, pg_get_indexdef(c2.oid) FROM pg_index i JOIN pg_class c2 ON (i.indexrelid = c2.oid AND c2.relkind = 'i') JOIN pg_namespace n2 ON (c2.relnamespace = n2.oid) JOIN pg_class c1 ON (i.indrelid = c1.oid) JOIN pg_namespace n1 ON (c1.relnamespace = n1.oid) LEFT JOIN pg_constraint con ON (c2.oid = con.conindid) WHERE con.conname IS NULL AND n1.nspname = ? AND c1.relname = ?"))
            {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        writer.write(rs.getString(3) + ";\n\n");
                    }
                }
            }
            // triggers
            try (PreparedStatement stmt = this.connection.prepareStatement("SELECT t.tgname, pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class r ON (t.tgrelid = r.oid) JOIN pg_namespace n ON (r.relnamespace = n.oid) WHERE (NOT t.tgisinternal) AND n.nspname = ? AND r.relname = ?"))
            {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        writer.write(rs.getString(2) + ";\n\n");
                    }
                }
            }
        }
    }
    
    public List<File> dumpFunctions(String schema, File to) throws SQLException, IOException
    {
        List<File> files = new LinkedList<File>();
        try (PreparedStatement stmt = this.connection.prepareStatement("SELECT pg_get_functiondef(p.oid), p.proname, (SELECT string_agg((SELECT t.typname FROM pg_type t WHERE t.oid = u.v), '_') AS argtypes FROM unnest(p.proargtypes) u(v)), pg_get_userbyid(p.proowner), pg_get_function_identity_arguments(p.oid)  FROM pg_proc p JOIN pg_namespace n ON (p.pronamespace = n.oid) WHERE n.nspname = ?"))
        {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    String def    = rs.getString(1);
                    String name   = rs.getString(2);
                    String args   = rs.getString(3);
                    String owner  = rs.getString(4);
                    String idargs = rs.getString(5);
                    // write out the def
                    File functionFile = new File (to, name + (args == null ? "" : "_" + args) + ".sql");
                    files.add(functionFile);
                    try (Writer fw = new BufferedWriter(new FileWriter(functionFile)))
                    {
                        fw.write(def + ";\n\n");
                        fw.write("ALTER FUNCTION " + schema + "." + name + "(" + idargs + ") OWNER TO " + owner + ";\n\n");
                    }
                    files.add(functionFile);
                }
            }
        }
        return files;
    }
    
    public List<File> dumpTypes(String schema, File outDir) throws SQLException, IOException
    {
        List<File> files = new LinkedList<File>();
        try (PreparedStatement stmt = this.connection.prepareStatement("SELECT t.typname, pg_get_userbyid(t.typowner) FROM pg_type t JOIN pg_namespace tn ON (t.typnamespace = tn.oid) JOIN pg_class r ON (t.typrelid = r.oid) WHERE t.typtype = 'c' AND r.relkind = 'c' AND tn.nspname = ?"))
        {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    String name = rs.getString(1);
                    String owner = rs.getString(2);
                    // dump the table
                    File typeFile = new File (outDir, name + ".sql");
                    files.add(typeFile);
                    this.dumpType(schema, name, owner, typeFile);
                }
            }
        }
        return files;
    }
    
    public void dumpType(String schema, String type, String owner, File to) throws SQLException, IOException
    {
        try (Writer writer = new BufferedWriter(new FileWriter(to)))
        {
            writer.write("CREATE TYPE \"" + schema + "\".\"" + type + "\" AS (\n");
            boolean ns = false;
            // get the columns
            try (PreparedStatement stmt = this.connection.prepareStatement("SELECT a.attnum, quote_ident(a.attname), format_type(a.atttypid, a.atttypmod) FROM pg_type t JOIN pg_namespace n ON (n.oid = t.typnamespace) JOIN pg_class c ON (t.typrelid = c.oid) JOIN pg_attribute a ON (a.attrelid = c.oid) WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 ORDER BY a.attnum"))
            {
                stmt.setString(1, schema);
                stmt.setString(2, type);
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        String aname = rs.getString(2);
                        String atype = rs.getString(3);
                        if (ns) writer.write(",\n");
                        writer.write("  " + aname + " " + atype.toUpperCase());
                        ns = true;
                    }
                }
            }
            // end table
            writer.write("\n);\n\n");
            writer.write("ALTER TYPE \"" + schema + "\".\"" + type + "\" OWNER TO " + owner + ";\n\n");
        }
    }
    
    public void close()
    {
        if (this.connection != null)
        {
            try
            {
                this.connection.close();
            }
            catch (Exception e)
            {
            }
        }
    }
}
