package com.intrbiz.schema.pretty;

import java.io.File;
import java.sql.DriverManager;

public class SchemaDump 
{
    public static void main( String[] args ) throws Exception
    {
        // args
        if (args.length != 4)
        {
            System.err.println("Usage: SchemaDump '<out_dir>' '<url>' '<username>' '<password>'");
            System.exit(1);
        }
        String outDir = args[0];
        String url    = args[1];
        String user   = args[2];
        String pass   = args[3];
        // load drivers
        DriverManager.registerDriver(new org.postgresql.Driver());
        // dump
        try (Dumper dumper = new Dumper(url, user, pass))
        {
            dumper.dump(new File(outDir));
        }
    }
}
