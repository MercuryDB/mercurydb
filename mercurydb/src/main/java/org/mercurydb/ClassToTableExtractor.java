package org.mercurydb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.mercurydb.queryutils.FieldExtractable;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.collect.Sets;

public class ClassToTableExtractor {
    public final Class<?> c;
    
    public final boolean hasSuper;
    
    public final String cSuper;
    
    public Collection<String> subClasses;
    
    public List<FieldData> fields;
    
    public String packageName;
    
    public List<QueryData> queries;

    public String tableSuffix;
    
    public int joinId;
    
    public List<TemplateCounter> templateCounters;
    
    public ClassToTableExtractor(
    		Class<?> c, String superTable, Collection<String> subClassTables, String tableSuffix, int joinId) 
    		throws IOException {
        this.c = c;
        this.hasSuper = superTable != null;
        this.cSuper = superTable;
        this.tableSuffix = tableSuffix;
        this.joinId = joinId;
        
        this.fields = new ArrayList<>();
        this.queries = new ArrayList<>();
        
        this.subClasses = subClassTables;
        populateFieldsList();
        populateQueriesList(queries, fields);
        templateCounters = new ArrayList<>(fields.size());
        for (int i = 1; i <= fields.size(); ++i) {
        	templateCounters.add(new TemplateCounter(i));
        }
    }
    
    private void populateFieldsList() {
        for (Field f : c.getFields()) {
        	// Ignore fields belonging to superclasses
        	if (!f.getDeclaringClass().equals(c)) continue;
        	
            String type = f.getType().getName();
            String fieldName = f.getName();
            boolean hasIndex = hasIndexAnnotation(f);
            boolean isFinal = Modifier.isFinal(f.getModifiers());
            fields.add(new FieldData(type, fieldName, hasIndex, isFinal));
        }
    }
    
    private static void populateQueriesList(List<QueryData> queries, List<FieldData> fields) {
    	Set<FieldData> fieldSet = new HashSet<>(fields);
    	Set<Set<FieldData>> powerset = Sets.powerSet(fieldSet);
    	for (Set<FieldData> querySet : powerset) {
    		// TODO: don't calculate all sets and allow 5 to be a runtime parameter
    		if (querySet.size() > 1 && querySet.size() <= 5) {
    			queries.add(new QueryData(querySet));
    		}
    	}
    }
    
    @SuppressWarnings("unused")
    private static class QueryData {
    	TreeSet<FieldData> qFields;
    	
    	QueryData(Collection<FieldData> fields) {
    		this.qFields = new TreeSet<FieldData>(fields);
    	}
    	
    	String prototype() {
    		String result = "";
    		for (FieldData fd : qFields) {
    			result += fd.type() + " " + fd.name + ", ";
    		}
    		return result.substring(0, result.length()-2);
    	}
    	
    	boolean hasIndex() {
    		for (FieldData fd : qFields) {
    			if (fd.hasIndex) return true;
    		}
    		return false;
    	}
    }

    
    @SuppressWarnings("unused")
    private static class FieldData implements Comparable<FieldData> {
        String _type;
        String name;
        boolean hasIndex;
        boolean isFinal;
        
        FieldData(String type, String fieldName, boolean hasIndex, boolean isFinal) {
        	this._type = type;
            this.name = fieldName;
            this.hasIndex = hasIndex;
            this.isFinal = isFinal;
        }
        
        FieldData(String type, String fieldName, boolean hasIndex) {
            this(type, fieldName, hasIndex, false);
        }
        
        FieldData(String type, String fieldName) {
            this(type,fieldName,false);
        }
        
        String type() {
            switch (_type) {
            case "boolean": return "Boolean";
            case "byte": return "Byte";
            case "char": return "Character";
            case "short": return "Short";
            case "int": return "Integer";
            case "long": return "Long";
            case "float": return "Float";
            case "double": return "Double";
            default: return _type;
            }
        }
        
        String CCname() {
        	return Utils.upperFirst(name);
        }

		@Override
		public int compareTo(FieldData o) {
			return name.compareTo(o.name);
		}
    }
    
    @SuppressWarnings("unused")
    private class TemplateCounter {
    	public List<Integer> counter;
    	
    	public TemplateCounter(int size) {
    		counter = new ArrayList<Integer>(size);
    		for (int i = 1; i <= size; ++i) {
    			counter.add(i);
    		}
    	}
    	
    	public String template() {
    		StringBuilder sb = new StringBuilder();
    		sb.append("<");
    		for (Integer i : counter) {
    			sb.append("F"+i+",");
    		}
    		sb.setCharAt(sb.length()-1, '>');
    		return sb.toString();
    	}
    	
    	public String prototype() {
    		StringBuilder sb = new StringBuilder();
    		for (Integer i : counter) {
    			sb.append(FieldExtractable.class.getSimpleName());
    			sb.append("<"+sourceClass()+", F"+i+"> fe"+i+", F"+i+" val"+i+",");
    		}
    		sb.setLength(sb.length()-1);
    		return sb.toString();
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
        outFile.getParentFile().mkdirs();
        PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
        this.packageName = packageName;
        extract(w);
        w.close();
    }

    private void extract(PrintWriter w) throws IOException {
        MustacheFactory mf = new DefaultMustacheFactory();
        String templateName = "template.java.mustache";
        InputStream templateStream = getClass().getResourceAsStream('/'+ templateName);
        Mustache template = mf.compile(new InputStreamReader(templateStream), templateName);
        Writer execute = template.execute(w, this);
        execute.flush();
    }

    private boolean hasIndexAnnotation(Field f) {
        return f.getAnnotation(org.mercurydb.HgIndex.class) != null;
    }
}