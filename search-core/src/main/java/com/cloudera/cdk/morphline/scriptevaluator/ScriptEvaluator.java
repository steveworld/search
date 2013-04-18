/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.morphline.scriptevaluator;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.cloudera.cdk.morphline.scriptengine.java.FastJavaScriptEngine;

/**
 * Creates and compiles the given Java statements, wrapped into a Java method with the given return
 * type and parameter types, along with a Java class definition that contains the given import
 * statements.
 * 
 * Compilation is done in main memory, i.e. without writing to the filesystem.
 * 
 * The result is an object that can be executed (and reused) any number of times. This is a high
 * performance implementation, using an optimized variant of https://scripting.dev.java.net/" (JSR
 * 223 Java Scripting). Calling {@link #evaluate(Object...)} just means calling
 * {@link Method#invoke(Object, Object...)} and as such has the same minimal runtime cost, i.e.
 * O(100M calls/sec/core).
 * 
 * Instances of this class are thread-safe if the user provided script statements are thread-safe.
 */
public class ScriptEvaluator<T> {

	private final FastJavaScriptEngine.JavaCompiledScript compiledScript;
	private final String javaStatements;
	private final String parseLocation;
	
	private static final AtomicLong nextClassNum = new AtomicLong();
	
	private static final String METHOD_NAME = "eval";

	public ScriptEvaluator(String javaImports, String javaStatements, Class<T> returnType,
			String[] parameterNames, Class[] parameterTypes,
			String parseLocation) throws ScriptException {
		
		if (parameterNames.length != parameterTypes.length) { 
			throw new IllegalArgumentException(
				"Lengths of parameterNames (" + parameterNames.length
						+ ") and parameterTypes (" + parameterTypes.length
						+ ") do not match"); 
		}
		
		this.javaStatements = javaStatements;
		this.parseLocation = parseLocation;		
		String myPackageName = getClass().getName();
		myPackageName = myPackageName.substring(0, myPackageName.lastIndexOf('.'));
		String className = "MyJavaClass" + nextClassNum.incrementAndGet();
		String returnTypeName = (returnType == Void.class ? "void" : returnType.getCanonicalName());
		
		String script = 
			"package " + myPackageName + ".scripts;"
			+ "\n"
			+ javaImports
			+ "\n"
			+ "\n public final class " + className + " {"		
			+ "\n	 public static " + returnTypeName + " " + METHOD_NAME + "(";
		
		for (int i = 0; i < parameterNames.length; i++) {
			if (i > 0) {
				script += ", ";
			}
			script += parameterTypes[i].getCanonicalName() + " " + parameterNames[i];
		}
		script += ") { " + javaStatements + " }";		 
		script += "\n }";
//		System.out.println(script);
		
		FastJavaScriptEngine engine = new FastJavaScriptEngine();
		StringWriter errorWriter = new StringWriter();
		engine.getContext().setErrorWriter(errorWriter);
		engine.getContext().setAttribute(ScriptEngine.FILENAME, className + ".java", ScriptContext.ENGINE_SCOPE);
		engine.getContext().setAttribute("parentLoader", getClass().getClassLoader(), ScriptContext.ENGINE_SCOPE);
		
		try {
			compiledScript = (FastJavaScriptEngine.JavaCompiledScript) engine.compile(script, METHOD_NAME, parameterTypes);
		} catch (ScriptException e) {
			String errorMsg = errorWriter.toString();
			if (errorMsg.length() > 0) 
				errorMsg = ": " + errorMsg;
			
			throwScriptSyntaxException(parseLocation, e.getMessage() + errorMsg);
			throw null; // keep compiler happy
		}
		
		engine.getContext().setErrorWriter(new PrintWriter(System.err, true)); // reset
	}
	
	public T evaluate(Object... params) {
		// TODO: consider restricting permissions/sandboxing; also see http://worldwizards.blogspot.com/2009/08/java-scripting-api-sandbox.html
		try {
			return (T) compiledScript.eval(params);
		} catch (ScriptException e) {				
			throwScriptException(parseLocation + " near: '" + javaStatements + "'", params, e);
		}
		return null; // keep compiler happy
	}

	private static void throwScriptSyntaxException(String parseLocation, String msg) {
		throwScriptSyntaxException(parseLocation, msg, null);
	}
	
	private static void throwScriptSyntaxException(String parseLocation, String msg, Throwable t) {
		if (t == null)
			throw new IllegalArgumentException("Cannot parse script: " + parseLocation + " caused by " + msg);
		else
			throw new IllegalArgumentException("Cannot parse script: " + parseLocation + " caused by " + msg, t);
	}
	
	private static void throwScriptException(String parseLocation, Object[] params, Throwable e) {
		throw new IllegalArgumentException("Cannot execute script: " + parseLocation + " for params " + Arrays.asList(params).toString(), e);
	}
	
}
