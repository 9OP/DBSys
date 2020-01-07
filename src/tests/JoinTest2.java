package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
// originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

/**
 * Here is the implementation for the tests. There are N tests performed. We start off by showing
 * that each operator works on its own. Then more complicated trees are constructed. As a nice
 * feature, we allow the user to specify a selection condition. We also allow the user to hardwire
 * trees together.
 */


// Define the R schema
class R {
  public int int1;
  public int int2;
  public int int3;
  public int int4;

  public R(int _int1, int _int2, int _int3, int _int4) {
    int1 = _int1;
    int2 = _int2;
    int3 = _int3;
    int4 = _int4;
  }
}


// Define the S schema
class S {
    public int int1;
    public int int2;
    public int int3;
    public int int4;
  
    public S(int _int1, int _int2, int _int3, int _int4) {
      int1 = _int1;
      int2 = _int2;
      int3 = _int3;
      int4 = _int4;
    }
  }


class JoinsDriver implements GlobalConst {

  private boolean OK = true;
  private boolean FAIL = false;
  private Vector S;
  private Vector R;

  /**
   * Constructor
   */

  public void populateData(String pathtodata, String filename, Vector table) {
    pathtodata = new File(".").getAbsolutePath() + pathtodata;
    BufferedReader reader;
    try {
			reader = new BufferedReader(new FileReader(
					pathtodata + filename));
      String line = reader.readLine();
      if (line != null) {
        line = reader.readLine();  // don't parse the headers
      }
			while (line != null) {
        String[] tableAttrs = line.trim().split(",");
        int[] parsedAttrs = new int[tableAttrs.length];
        for (int i=0; i < tableAttrs.length; ++i) {
          parsedAttrs[i] = Integer.parseInt(tableAttrs[i]);
        }
        if (filename.split(".")[0] == "R") {
          table.addElement(new R(parsedAttrs[0], parsedAttrs[1], parsedAttrs[2], parsedAttrs[3]));
        } else {
          table.addElement(new S(parsedAttrs[0], parsedAttrs[1], parsedAttrs[2], parsedAttrs[3]));
        }
				// read next line
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
  }

  public JoinsDriver() {

    // build Sailor, Boats, Reserves table
    R = new Vector();
    S = new Vector();

    String pathtodata="../../QueriesData_newvalues/";

    populateData(pathtodata, "S.txt", S);
    populateData(pathtodata, "R.txt", R);


    boolean status = OK;
    int numS = S.size();
    int numS_attrs = 4;
    int numR = R.size();
    int numR_attrs = 4;

    String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointest2db";
    String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog2";

    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    String remove_joincmd = remove_cmd + dbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    } catch (IOException e) {
      System.err.println("" + e);
    }

    /*
     * ExtendedSystemDefs extSysDef = new ExtendedSystemDefs( "/tmp/minibase.jointestdb",
     * "/tmp/joinlog", 1000,500,200,"Clock");
     */

    SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");


    // creating the S relation
    AttrType[] Stypes = new AttrType[4];
    Stypes[0] = new AttrType(AttrType.attrInteger);
    Stypes[1] = new AttrType(AttrType.attrInteger);
    Stypes[2] = new AttrType(AttrType.attrInteger);
    Stypes[3] = new AttrType(AttrType.attrInteger);

    // SOS
    short[] Ssizes = new short[1];
    Ssizes[0] = 30; // first elt. is 30

    Tuple t = new Tuple();
    try {
      t.setHdr((short) 4, Stypes, Ssizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    int size = t.size();

    // inserting the tuple into file "S"
    RID rid;
    Heapfile f = null;
    try {
      f = new Heapfile("S.in");
    } catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }

    t = new Tuple(size);
    try {
      t.setHdr((short) 4, Stypes, Ssizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    for (int i = 0; i < numS; i++) {
      try {
        t.setIntFld(1, ((S) S.elementAt(i)).int1);
        t.setIntFld(2, ((S) S.elementAt(i)).int2);
        t.setIntFld(3, ((S) S.elementAt(i)).int3);
        t.setIntFld(4, ((S) S.elementAt(i)).int4);
      } catch (Exception e) {
        System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
        status = FAIL;
        e.printStackTrace();
      }

      try {
        rid = f.insertRecord(t.returnTupleByteArray());
      } catch (Exception e) {
        System.err.println("*** error in Heapfile.insertRecord() ***");
        status = FAIL;
        e.printStackTrace();
      }
    }
    if (status != OK) {
      // bail out
      System.err.println("*** Error creating relation for S");
      Runtime.getRuntime().exit(1);
    }

    // creating the R relation
    AttrType[] Rtypes = new AttrType[4];
    Rtypes[0] = new AttrType(AttrType.attrInteger);
    Rtypes[1] = new AttrType(AttrType.attrInteger);
    Rtypes[2] = new AttrType(AttrType.attrInteger);
    Rtypes[3] = new AttrType(AttrType.attrInteger);

    short[] Rsizes = new short[1];
    Rsizes[0] = 15;
    t = new Tuple();
    try {
      t.setHdr((short) 3, Rtypes, Rsizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    size = t.size();

    // inserting the tuple into file "boats"
    // RID rid;
    f = null;
    try {
      f = new Heapfile("R.in");
    } catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }

    t = new Tuple(size);
    try {
      t.setHdr((short) 4, Rtypes, Rsizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    for (int i = 0; i < numR; i++) {
      try {
        t.setIntFld(1, ((R) R.elementAt(i)).int1);
        t.setIntFld(2, ((R) R.elementAt(i)).int2);
        t.setIntFld(3, ((R) R.elementAt(i)).int3);
        t.setIntFld(4, ((R) R.elementAt(i)).int4);

      } catch (Exception e) {
        System.err.println("*** error in Tuple.setStrFld() ***");
        status = FAIL;
        e.printStackTrace();
      }

      try {
        rid = f.insertRecord(t.returnTupleByteArray());
      } catch (Exception e) {
        System.err.println("*** error in Heapfile.insertRecord() ***");
        status = FAIL;
        e.printStackTrace();
      }
    }
    if (status != OK) {
      // bail out
      System.err.println("*** Error creating relation for R");
      Runtime.getRuntime().exit(1);
    }
  }



  public boolean runTests() {

    Disclaimer();
    Query2();

    System.out.print("Finished joins testing" + "\n");

    return true;
  }

  private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

    expr[0].next = null;
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
    expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

    expr[1] = null;

    expr2[0].next = null;
    expr2[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
    expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr2[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

    expr2[1].op = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].next = null;
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
    expr2[1].type2 = new AttrType(AttrType.attrString);
    expr2[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
    expr2[1].operand2.string = "red";

    expr2[2] = null;
  }

  public void Query2() {
    System.out.print("**********************Query2 strating *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
    System.out.print("Query: Find the names of sailors who have reserved " + "a red boat\n"
        + "       and return them in alphabetical order.\n\n" + "  SELECT   S.sname\n"
        + "  FROM     Sailors S, Boats B, Reserves R\n"
        + "  WHERE    S.sid = R.sid AND R.bid = B.bid AND B.color = 'red'\n"
        + "  ORDER BY S.sname\n" + "Plan used:\n" + " Sort (Pi(sname) (Sigma(B.color='red')  "
        + "|><|  Pi(sname, bid) (S  |><|  R)))\n\n"
        + "(Tests File scan, Index scan ,Projection,  index selection,\n "
        + "sort and simple nested-loop join.)\n\n");

    // Build Index first
    IndexType b_index = new IndexType(IndexType.B_Index);


    // ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
    // }
    // catch (Exception e) {
    // e.printStackTrace();
    // System.err.print ("Failure to add index.\n");
    // Runtime.getRuntime().exit(1);
    // }



    CondExpr[] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();

    CondExpr[] outFilter2 = new CondExpr[3];
    outFilter2[0] = new CondExpr();
    outFilter2[1] = new CondExpr();
    outFilter2[2] = new CondExpr();

    Query2_CondExpr(outFilter, outFilter2);
    Tuple t = new Tuple();
    t = null;

    AttrType[] Stypes = {new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrReal)};

    AttrType[] Stypes2 = {new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),};

    short[] Ssizes = new short[1];
    Ssizes[0] = 30;
    AttrType[] Rtypes = {new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrString),};

    short[] Rsizes = new short[1];
    Rsizes[0] = 15;
    AttrType[] Btypes = {new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrString),};

    short[] Bsizes = new short[2];
    Bsizes[0] = 30;
    Bsizes[1] = 20;
    AttrType[] Jtypes = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger),};

    short[] Jsizes = new short[1];
    Jsizes[0] = 30;
    AttrType[] JJtype = {new AttrType(AttrType.attrString),};

    short[] JJsize = new short[1];
    JJsize[0] = 30;
    FldSpec[] proj1 =
        {new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 2)}; // S.sname,
                                                                                                     // R.bid

    FldSpec[] proj2 = {new FldSpec(new RelSpec(RelSpec.outer), 1)};

    FldSpec[] Sprojection =
        {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2),
        // new FldSpec(new RelSpec(RelSpec.outer), 3),
        // new FldSpec(new RelSpec(RelSpec.outer), 4)
        };

    CondExpr[] selects = new CondExpr[1];
    selects[0] = null;


    // IndexType b_index = new IndexType(IndexType.B_Index);
    iterator.Iterator am = null;


    // _______________________________________________________________
    // *******************create an scan on the heapfile**************
    // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
    Tuple tt = new Tuple();
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    } catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    } catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile f = null;
    try {
      f = new Heapfile("sailors.in");
    } catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    Scan scan = null;

    try {
      scan = new Scan(f);
    } catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
    } catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    RID rid = new RID();
    int key = 0;
    Tuple temp = null;

    try {
      temp = scan.getNext(rid);
    } catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while (temp != null) {
      tt.tupleCopy(temp);

      try {
        key = tt.getIntFld(1);
      } catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }

      try {
        btf.insert(new IntegerKey(key), rid);
      } catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }

      try {
        temp = scan.getNext(rid);
      } catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }
    }

    // close the file scan
    scan.closescan();


    // _______________________________________________________________
    // *******************close an scan on the heapfile**************
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    System.out.print("After Building btree index on sailors.sid.\n\n");
    try {
      am = new IndexScan(b_index, "sailors.in", "BTreeIndex", Stypes, Ssizes, 4, 2, Sprojection,
          null, 1, false);
    }

    catch (Exception e) {
      System.err.println("*** Error creating scan for Index scan");
      System.err.println("" + e);
      Runtime.getRuntime().exit(1);
    }


    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins(Stypes2, 2, Ssizes, Rtypes, 3, Rsizes, 10, am, "reserves.in",
          outFilter, null, proj1, 2);
    } catch (Exception e) {
      System.err.println("*** Error preparing for nested_loop_join");
      System.err.println("" + e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    NestedLoopsJoins nlj2 = null;
    try {
      nlj2 = new NestedLoopsJoins(Jtypes, 2, Jsizes, Btypes, 3, Bsizes, 10, nlj, "boats.in",
          outFilter2, null, proj2, 1);
    } catch (Exception e) {
      System.err.println("*** Error preparing for nested_loop_join");
      System.err.println("" + e);
      Runtime.getRuntime().exit(1);
    }

    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    Sort sort_names = null;
    try {
      sort_names = new Sort(JJtype, (short) 1, JJsize, (iterator.Iterator) nlj2, 1, ascending,
          JJsize[0], 10);
    } catch (Exception e) {
      System.err.println("*** Error preparing for nested_loop_join");
      System.err.println("" + e);
      Runtime.getRuntime().exit(1);
    }

  }


  private void Disclaimer() {
    System.out.print("\n\nAny resemblance of persons in this database to"
        + " people living or dead\nis purely coincidental. The contents of "
        + "this database do not reflect\nthe views of the University,"
        + " the Computer  Sciences Department or the\n" + "developers...\n\n");
  }
}


public class JoinTest2 {
  public static void main(String argv[]) {
    boolean sortstatus;
    // SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    // JavabaseDB.openDB("/tmp/nwangdb", 5000);

    JoinsDriver jjoin = new JoinsDriver();
    System.out.println("JoinTest2 start...");
    sortstatus = jjoin.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    } else {
      System.out.println("join tests completed successfully");
    }
  }
}

