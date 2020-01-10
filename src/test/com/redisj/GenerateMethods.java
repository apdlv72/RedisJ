package com.redisj;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

public class GenerateMethods {

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        // lynx -dump "https://redis.io/commands" | \
        // grep --colour=never '\* \[' | sed 's/^ *//;' | sed -E 's/^[^A-Z]*//' >> commands
    }

    @Test
    public void testGenerateMethods() throws IOException {
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
}
