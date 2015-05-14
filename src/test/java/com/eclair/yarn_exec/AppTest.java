package com.eclair.yarn_exec;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;



/**
 * Unit test for simple App.
 */
public class AppTest
    extends TestCase
{

    public AppTest( String testName )
    {
        super( testName );
        System.out.println(String.format("%1$03d", 24));
    }


    @Test
    public void testApp()
    {
        List<? super B> list = new ArrayList<B>();
        list.add(new C());
        assertEquals("Not Same class", C.class, list.get(0).getClass());
    }

    static class A{

    }

    static class B extends A{

    }
    static class C extends B{

    }
}
