package org.mercurydb;

import java.io.IOException;

import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class ClassModifier {
	private CtClass _srcClass;
	private String _tableClass; 
	
	public ClassModifier(CtClass srcClass, String tableClass) {
		_srcClass = srcClass;
		_tableClass = tableClass;
	}

	public void modify() throws CannotCompileException, IOException, NotFoundException {
		String constructorHook = _tableClass + ".insert(this);";
		for (CtConstructor con : _srcClass.getConstructors()) {
			con.insertAfter(constructorHook);
		}
		
		for (CtField cf : _srcClass.getFields()) {
			if (!cf.hasAnnotation(org.mercurydb.Index.class)) continue;
			
			String methodName = "set" + Utils.upperFirst(cf.getName());
			try {
				// If method does not exist. This statement will throw an exception
				CtMethod cm = _srcClass.getDeclaredMethod(methodName);
				// If we get this far, add the hook
				cm.insertAfter(_tableClass + "." + methodName + "(this, " + cf.getName() + ");");
			} catch (NotFoundException e) {
				System.err.println("Warning: No set method found for indexed field " + _srcClass.getName() + "." + cf.getName());
			} catch (SecurityException e) {
				System.err.println("Warning: " + e.getMessage());
			}
		}
		CtMethod method = CtNewMethod.make("public void fooey() {}", _srcClass);
		_srcClass.addMethod(method);
		_srcClass.writeFile("build/classes/main");
	}
}
