package com.itmo.java.basics;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.impl.*;
import com.itmo.java.basics.logic.Database;

public class Main {

    /**
     This code is used only for debugging purposes
     It is not maintained and should not be given an overview
     **/

    public static void main(String[] args) {

//        try {
//
//            ExecutionEnvironment ctx = new ExecutionEnvironmentImpl(new DatabaseConfig());
//            InitializationContext initContext = new InitializationContextImpl(ctx, null, null, null);
//            Initializer initializer =
//                    new DatabaseServerInitializer(
//                            new DatabaseInitializer(
//                                    new TableInitializer(
//                                            new SegmentInitializer())));
//
//            initializer.perform(initContext);
//
//            Database d = initContext.executionEnvironment().getDatabase("test_db").get();
//
//            System.out.println(new String(d.read("table1", "a").get()));
//            for (int i = 0; i < 16000; i++) {
//                d.write("table1", "a", ("" + i).getBytes());
//            }
//
//        } catch (DatabaseException e) {
//            e.printStackTrace();
//        }

//        try {

            // create db directory
//            if (Files.exists(Paths.get(".\\test_db\\"))) {
//                Files.delete(Paths.get(".\\test_db\\"));
//            }

//            Path db = Paths.get(".\\test_db" + System.currentTimeMillis() + "\\");
//            File f = new File(db.toString());
//            if (!f.mkdir()) {
//                throw new DatabaseException("DB directory can not be created.");
//            }
//
//            // testing
//            Database d = DatabaseImpl.create("test", db);
//            d.createTableIfNotExists("table1");
//            d.write("table1", "a", "a_value".getBytes());
//            d.write("table1", "b", "b_value".getBytes());
//
//            for (int i = 0; i < 16000; i++) {
//                d.write("table1", "a", ("" + i).getBytes());
//            }
//
//            System.out.println(new String(d.read("table1", "a").get()));
//            Optional<byte[]> s = d.read("table1", "stop");
//            s = d.read("table1", "stop");
//            System.out.println(s.get());
//
//        } catch (Exception e) {
//            System.out.println(e.toString());
//        }

//        try {
//            Path p = Paths.get(".");
//            Segment s = SegmentImpl.create(SegmentImpl.createSegmentName("testTable"), p);
//
//            Optional<byte[]> v = s.read("k");
//            System.out.println(v.isPresent());
//
//            String s1 = "123";
//            s.write("k", s1.getBytes());
//            v = s.read("k");
//            System.out.println(v.isPresent());
//            String s2 = new String(v.get());
//            System.out.println(s2);
//
//            for (int i = 0; !s.isReadOnly(); i++) {
//                s.write("k" + i, ("asqniuqrbquir" + i).getBytes());
//            }
//
//            System.out.println(new String(s.read("k").get()));
//            System.out.println(new String(s.read("k0").get()));
//            System.out.println(new String(s.read("k1").get()));
//            System.out.println(new String(s.read("k390").get()));
//
//        } catch (Exception e) {
//            System.out.println("Hm.");
//            System.out.println(e.toString());
//        }

    }

}
