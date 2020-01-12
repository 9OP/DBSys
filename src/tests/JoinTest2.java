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

  public void show() {
    System.out.print(int1);
    System.out.print(int2);
    System.out.print(int3);
    System.out.print(int4);
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

  public void show() {
    System.out.print(int1);
    System.out.print(int2);
    System.out.print(int3);
    System.out.print(int4);
  }
}


class JoinsDriver implements GlobalConst {

  private boolean OK = true;
  private boolean FAIL = false;
  private Vector s;
  private Vector r;
  // private String
  // pathtodata="/media/chaoticdenim/DATA/Work/3A/EURECOM/DBSys/Assignment/QueriesData_newvalues/";
  public String pathToData = new File("").getAbsolutePath();

  /**
   * Constructor
   */

  public void populateData(String pathtodata, String filename, Vector table) {
    BufferedReader reader;
    boolean isR = filename.split("\\.")[0].equals("R");
    boolean isS = filename.split("\\.")[0].equals("S");
    try {
      reader = new BufferedReader(
          new FileReader(pathToData + "/../../QueriesData_newvalues/" + filename));
      String line = reader.readLine();
      if (line != null) {
        line = reader.readLine(); // don't parse the headers
      }
      while (line != null) {
        String[] tableAttrs = line.trim().split(",");
        int[] parsedAttrs = new int[tableAttrs.length];
        for (int i = 0; i < tableAttrs.length; ++i) {
          parsedAttrs[i] = Integer.parseInt(tableAttrs[i]);
        }
        if (isR) { 
          table.addElement(new R(parsedAttrs[0], parsedAttrs[1], parsedAttrs[2], parsedAttrs[3]));
        }
        else if (isS) {
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
    r = new Vector();
    s = new Vector();

    populateData(pathToData, "S.txt", s);
    populateData(pathToData, "R.txt", r);

    boolean status = OK;
    int numS = s.size();
    int numS_attrs = 4;
    int numR = r.size();
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
        t.setIntFld(1, ((S) s.elementAt(i)).int1);
        t.setIntFld(2, ((S) s.elementAt(i)).int2);
        t.setIntFld(3, ((S) s.elementAt(i)).int3);
        t.setIntFld(4, ((S) s.elementAt(i)).int4);
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
    Rsizes[0] = 30;
    t = new Tuple();
    try {
      t.setHdr((short) 4, Rtypes, Rsizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    size = t.size();

    // inserting the tuple into file "R"
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
        t.setIntFld(1, ((R) r.elementAt(i)).int1);
        t.setIntFld(2, ((R) r.elementAt(i)).int2);
        t.setIntFld(3, ((R) r.elementAt(i)).int3);
        t.setIntFld(4, ((R) r.elementAt(i)).int4);

      } catch (Exception e) {
        //System.err.println("*** error in Tuple.setStrFld() ***");
        status = FAIL;
        //e.printStackTrace();
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

    // Disclaimer();
    try {
      Query1_a();
    } catch (FileNotFoundException ex) {
      ;
    } catch (IOException ex) {
      ;
    }
    // Query2();

    System.out.print("Finished joins testing" + "\n");

    return true;
  }

  public void Query1_a() throws FileNotFoundException, IOException {
    // Single predicate query
    // LINE 1: Rel1 col# Rel2 col#
    // LINE 2: Rel1 Rel2
    // LINE 3: Rel1 col# op 1 Rel2 col#
    System.out.println("**********************Query1_a starting *********************");
    boolean status = OK;
    String selectRel1 = "", selectRel2 = "", selectRel1Col = "", selectRel2Col = "";
    String rel1 = "", rel2 = "";
    String whereRel1 = "", whereRel2 = "", whereRel1Col = "", whereRel2Col = "";
    Integer op = 0;
    String[] op_string = {"=", "<", ">", "!=", ">=", "<="};

    try {
      BufferedReader query = new BufferedReader(
          new FileReader(pathToData + "/../../QueriesData_newvalues/query_1a.txt"));
      // Line1
      String[] line1 = query.readLine().split(" ");
      selectRel1 = line1[0].split("_")[0];
      selectRel2 = line1[1].split("_")[0];
      selectRel1Col = line1[0].split("_")[1];
      selectRel2Col = line1[1].split("_")[1];
      // Line2
      String[] line2 = query.readLine().split(" ");
      rel1 = line2[0];
      rel2 = line2[1];
      // Line3
      String[] line3 = query.readLine().split(" ");
      op = Integer.parseInt(line3[1]);
      whereRel1 = line3[0].split("_")[0];
      whereRel2 = line3[2].split("_")[0];
      whereRel1Col = line3[0].split("_")[1];
      whereRel2Col = line3[2].split("_")[1];

      query.close();
    } catch (FileNotFoundException ex) {
      ex.printStackTrace();
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    System.out.print(
        "  SELECT   " + selectRel1 + "." + selectRel1Col + " " + selectRel2 + "." + selectRel2Col
            + "\n" + "  FROM     " + rel1 + " " + rel2 + "\n" + "  WHERE    " + whereRel1 + "."
            + whereRel1Col + " " + op_string[op] + " " + whereRel2 + "." + whereRel2Col + "\n");

    // Build Index first
    IndexType b_index = new IndexType(IndexType.B_Index);

    CondExpr[] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();

    outFilter[0].next = null;
    outFilter[0].op = new AttrOperator(op);
    outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
    outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
    outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), Integer.parseInt(whereRel1Col));
    outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), Integer.parseInt(whereRel2Col));

    Tuple t = new Tuple();
    t = null;

    AttrType[] Stypes = {new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};
    AttrType[] Stypes2 = {new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), };
    short[] Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType[] Rtypes2 = {new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), };
    short[] Rsizes = new short[1];
    Rsizes[0] = 30;

    AttrType[] Jtypes = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger),};
    short[] Jsizes = new short[1];
    Jsizes[0] = 30;

    FldSpec[] proj1 =
        {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.innerRel), 1)}; // S.1,
                                                                                                     // R.1

    FldSpec[] Sprojection =
        {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 3),}; //column to project S
    
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
      f = new Heapfile("S.in");
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

    System.out.print("After Building btree index on S.sid\n\n");
    try {
      am = new IndexScan(b_index, "S.in", "BTreeIndex", Stypes, Ssizes, 4, 2, Sprojection,
          null, 1, false); //1: col to index in S
    }
    catch (Exception e) {
      System.err.println("*** Error creating scan for Index scan");
      System.err.println("" + e);
      Runtime.getRuntime().exit(1);
    }


    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins(Stypes2, 2, null, Rtypes2, 2, null, 10, am, "R.in",
          outFilter, null, proj1, 2);
    } catch (Exception e) {
      System.err.println("*** Error preparing for nested_loop_join");
      System.err.println("" + e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    t = null;
    try {
      while ((t = nlj.get_next()) != null) {
        System.out.println("dans la boucle get_next");
        t.print(Jtypes);
      }
    } catch (Exception e) {
      System.err.println("" + e);
      e.printStackTrace();
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

