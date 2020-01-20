package com.redisj;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

public class GenerateMethods {

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        // lynx -dump "https://redis.io/commands" | \
        // grep --colour=never '\* \[' | sed 's/^ *//;' | sed -E 's/^[^A-Z]*//' >> commands
    }

    @Test
    public void testGenerateServerMethods() throws IOException {
        File file = new File("commands");
        FileReader rd = new FileReader(file);
        char cbuf[] = new char[(int)file.length()];
        rd.read(cbuf);
        rd.close();

        String string = new String(cbuf);
        String[] lines = string.split("[\r\n]");

        for (String line : lines) {

            String first = line.split(" ")[0];
            //System.out.println(first);

            String lower = first.toLowerCase();

            System.out.printf(
                    "        @CommandMethod(args = {}, since=\"1.0.0\")\n" +
                    "        protected void %s(Database db, Args args) throws IOException {\n" +
                    "            _todo(\"%s\");\n" +
                    "        }\n" +
                    "\n",
                    lower, lower);
        }
    }

    @Test
    public void testGenerateTestMethods() throws IOException {
        File file = new File("commands");
        FileReader rd = new FileReader(file);
        char cbuf[] = new char[(int)file.length()];
        rd.read(cbuf);
        rd.close();

        String string = new String(cbuf);
        String[] lines = string.split("[\r\n]");

        Set<String> done = new HashSet<String>();
        for (String line : lines) {

            String first = line.split(" ")[0];
            //System.out.println(first);

            String lower = first.toLowerCase();
            String camel = first.substring(0,1).toUpperCase() + lower.substring(1);
            if (done.contains(lower)) {
                continue;
            }
            done.add(lower);

            System.out.printf(
                    "        @Test\n" +
                    "        public void test%s() throws IOException {\n" +
                    "            Object actual = client.%s(\"key\", \"value\");\n" +
                    "            Object expect = null;\n" +
                    "            assertEquals(expect, actual);\n" +
                    "        }\n" +
                    "\n",
                    camel, lower);
        }
    }
}
