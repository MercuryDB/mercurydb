package org.mercurydb;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.collect.Sets;
import org.mercurydb.annotations.HgIndex;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

public class ClassToTableExtractor {
    public final Class<?> c;

    public final boolean hasSuper;

    public final String cSuper;

    public Collection<String> subClasses;

    public List<FieldData> fields;

    public String packageName;

    // TODO this field may be unnecessary
    public List<QueryData> queries;

    public String tableSuffix;

    public List<ConstructorData> constructors;

    public int joinId;

    public ClassToTableExtractor(
            Class<?> c, String superTable, Collection<String> subClassTables, String tableSuffix, int joinId)
            throws IOException {
        this.c = c;
        this.hasSuper = superTable != null;
        this.cSuper = superTable;
        this.tableSuffix = tableSuffix;
        this.joinId = joinId;

        this.fields = new ArrayList<FieldData>();
        this.queries = new ArrayList<QueryData>();
        this.constructors = new ArrayList<ConstructorData>();

        this.subClasses = subClassTables;
        populateFieldsList();
        populateQueriesList(queries, fields);
        //populateConstructorsList();
    }

    private void populateFieldsList() {
        for (Field f : c.getFields()) {
            // Ignore fields belonging to superclasses
            if (!f.getDeclaringClass().equals(c)) continue;
            fields.add(new FieldData(f));
        }
    }

    // TODO this function may be unneccesary
    private static void populateQueriesList(List<QueryData> queries, List<FieldData> fields) {
        Set<FieldData> fieldSet = new HashSet<FieldData>(fields);
        Set<Set<FieldData>> powerset = Sets.powerSet(fieldSet);
        for (Set<FieldData> querySet : powerset) {
            // TODO: don't calculate all sets and allow 5 to be a runtime parameter
            if (querySet.size() > 1 && querySet.size() <= 5) {
                queries.add(new QueryData(querySet));
            }
        }
    }

    // TODO unused for now, but should be used later in conjunction with ConstructorData
    @SuppressWarnings("unused")
    private void populateConstructorsList() {
        for (Constructor<?> con : c.getConstructors()) {
            constructors.add(new ConstructorData(con));
        }
    }

    private static class ConstructorData {
        public ConstructorData(Constructor<?> con) {
            // TODO implement
            throw new NotImplementedException();
//            for (Parameter p : con.getParameters()) {
//                System.out.println(p.getParameterizedType().getTypeName() + " " + p.getName());
//            }
        }
    }

    // TODO if the `queries` field is unnecessary, then this class may be unnecessary as well
    private static class QueryData {
        TreeSet<FieldData> qFields;

        QueryData(Collection<FieldData> fields) {
            this.qFields = new TreeSet<FieldData>(fields);
        }

        @SuppressWarnings("unused") // used in template
        String prototype() {
            StringBuilder result = new StringBuilder();
            for (FieldData fd : qFields) {
                result.append(String.format("%s %s, ", fd.type(), fd.name));
            }
            return result.substring(0, result.length() - 2);
        }

        @SuppressWarnings("unused") // used in template
        boolean hasIndex() {
            for (FieldData fd : qFields) {
                if (fd.hasIndex) return true;
            }
            return false;
        }
    }

    private static class FieldData implements Comparable<FieldData> {
        Field field;

        String rawType;
        String name;
        Type fieldType;

        boolean hasIndex;
        boolean isOrdered;
        boolean isFinal;

        FieldData(Field f) {
            field = f;

            fieldType = f.getGenericType();
            rawType = fieldType.getTypeName();
            name = f.getName();

            // fetch HgIndex annotation
            HgIndex indexAnnotation = getIndexAnnotation(f);
            if (indexAnnotation != null) {
                hasIndex = true;
                isOrdered = indexAnnotation.ordered() && Comparable.class.isAssignableFrom(normalizeType());
            }

            hasIndex = indexAnnotation != null;
            isFinal = Modifier.isFinal(f.getModifiers());
        }

        String type() {
            if (isPrimitiveType(rawType)) {
                return normalizeType().getName();
            } else {
                return rawType;
            }
        }

        /**
         * @return true if type is a String representing a primitive type.
         */
        boolean isPrimitiveType(String type) {
            switch (type) {
                case "boolean":
                case "byte":
                case "char":
                case "short":
                case "int":
                case "long":
                case "float":
                case "double":
                    return true;
                default:
                    return false; // it's an object
            }
        }

        Class<?> normalizeType() {
            if (rawType.equals("boolean")) {
                return Boolean.class;
            } else if (rawType.equals("byte")) {
                return Byte.class;
            } else if (rawType.equals("char")) {
                return Character.class;
            } else if (rawType.equals("short")) {
                return Short.class;
            } else if (rawType.equals("int")) {
                return Integer.class;
            } else if (rawType.equals("long")) {
                return Long.class;
            } else if (rawType.equals("float")) {
                return Float.class;
            } else if (rawType.equals("double")) {
                return Double.class;
            } else {
                return field.getType();
            }
        }

        @SuppressWarnings("unused") // used in template.java.mustache
        String CCname() {
            return Utils.upperFirst(name);
        }

        @Override
        @SuppressWarnings("NullableProblems") // parameter "o" can be null or non-null
        public int compareTo(FieldData o) {
            if (o == null) {
                return 1; // sort nulls last
            }

            return name.compareTo(o.name);
        }
    }

    public String fullSourceClass() {
        return c.getName();
    }

    public String sourceClass() {
        return c.getSimpleName();
    }

    public void extract(String outPath, String packageName) throws IOException {
        File outFile = new File(outPath);

        //ignore return value because true or false are both success AFAIC
        //noinspection ResultOfMethodCallIgnored
        outFile.getParentFile().mkdirs();

        PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
        this.packageName = packageName;
        extract(w);
        w.close();
    }

    private void extract(PrintWriter w) throws IOException {
        MustacheFactory mf = new DefaultMustacheFactory();
        String templateName = "template.java.mustache";
        InputStream templateStream = getClass().getResourceAsStream('/' + templateName);
        Mustache template = mf.compile(new InputStreamReader(templateStream), templateName);
        Writer execute = template.execute(w, this);
        execute.flush();
    }

    private static HgIndex getIndexAnnotation(Field f) {
        return f.getAnnotation(HgIndex.class);
    }
}
