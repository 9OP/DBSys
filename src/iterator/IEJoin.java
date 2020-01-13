package iterator;


import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.*;

/**
 *
 * This file contains an implementation of the IE Join algorithm as described in the VLDBJ paper.
 */

public class IEJoin extends Iterator {
    private AttrType _in1[];
    private int in1_len;
    private int inner_i, outer_i;
    private Sort outer;
    private Sort inner;
    private short t1_str_sizescopy[];
    private CondExpr OutputFilter[];
    private int n_buf_pgs; // # of buffer pages available.
    private boolean done, // Is the join complete
            get_from_outer; // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple; // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile hf;

    /**
     * constructor Initialize the two relations which are joined, including relation type,
     * 
     * @param in1          Array containing field types of R.
     * @param len_in1      # of columns in R.
     * @param t1_str_sizes shows the length of the string fields.
     * @param amt_of_mem   IN PAGES
     * @param relationName access hfapfile for right i/p to join
     * @param outFilter    select expressions
     * @param proj_list    shows what input fields go where in the output tuple
     * @param n_out_flds   number of outer relation fileds
     * @exception IOException         some I/O fault
     * @exception NestedLoopException exception from this class
     */
    public IEJoin(AttrType in1[], int len_in1, short t1_str_sizes[], int amt_of_mem, Iterator am,
            String relationName, CondExpr outFilter[], int num_records, FldSpec proj_list[],
            int n_out_flds) throws IOException, NestedLoopException {

        _in1 = new AttrType[in1.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        in1_len = len_in1;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        t1_str_sizescopy = t1_str_sizes;

        OutputFilter = outFilter;
        n_buf_pgs = amt_of_mem;
        inner = null;
        done = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;

        TupleOrder order;

        // Sort differently depending on the operator
        if (outFilter[0].op.attrOperator == AttrOperator.aopGT
                || outFilter[0].op.attrOperator == AttrOperator.aopGE) {
            order = new TupleOrder(TupleOrder.Descending);
        } else {
            order = new TupleOrder(TupleOrder.Ascending);
        }

        try {
            // sort the tuples on the attribute we are considering for the where clause
            //TODO: is this really only for the operand 1?
            outer = new Sort(in1, (short) len_in1, t1_str_sizes, am,
                    outFilter[0].operand1.symbol.offset, order, 30, n_buf_pgs);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in1, len_in1,
                    t1_str_sizes, t1_str_sizes, proj_list, nOutFlds);
        } catch (TupleUtilsException e) {
            throw new NestedLoopException(e, "TupleUtilsException is caught by IEJoin.java");
        }

        try {
            hf = new Heapfile(relationName);
        } catch (Exception e) {
            throw new NestedLoopException(e, "IEJoin: Create new heapfile failed.");
        }
    }

    /**
     * @return The joined tuple is returned
     * @exception IOException               I/O errors
     * @exception JoinsException            some join exception
     * @exception IndexException            exception from super class
     * @exception InvalidTupleSizeException invalid tuple size
     * @exception InvalidTypeException      tuple type not valid
     * @exception PageNotReadException      exception from lower layer
     * @exception TupleUtilsException       exception from using tuple utilities
     * @exception PredEvalException         exception from PredEval class
     * @exception SortException             sort exception
     * @exception LowMemException           memory error
     * @exception UnknowAttrType            attribute type unknown
     * @exception UnknownKeyTypeException   key type unknown
     * @exception Exception                 other exceptions
     * 
     */
    public Tuple get_next()
            throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
            InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException,
            SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        // This is a DUMBEST form of a join, not making use of any key information...

        if (done)
            return null;

        do {
            // If get_from_outer is true, Get a tuple from the outer, delete
            // an existing scan on the file, and reopen a new scan on the file.
            // If a get_next on the outer returns DONE?, then the nested loops
            // join is done too.

            if (get_from_outer == true) {
                get_from_outer = false;
                if (inner != null) // If this not the first time,
                {
                    inner = null;
                }

                try {
                    inner = outer; // TODO: this is for single predicate. change for others.
                } catch (Exception e) {
                    throw new NestedLoopException(e, "openScan failed");
                }
                if ((outer_tuple = outer.get_next()) == null) {
                    done = true;
                    if (inner != null) {
                        inner = null;
                    }
                    return null;
                }
            } // ENDS: if (get_from_outer == TRUE)

            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.

            while ((inner_tuple = inner.get_next()) != null) {
                inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
                if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in1) == true) {
                    // Apply a projection on the outer and inner tuples.
                    Projection.Join(outer_tuple, _in1, inner_tuple, _in1, Jtuple, perm_mat,
                            nOutFlds);
                    return Jtuple;
                }
            }

            // There has been no match. (otherwise, we would have
            // returned from t//he while loop. Hence, inner is
            // exhausted, => set get_from_outer = TRUE, go to top of loop

            get_from_outer = true; // Loop back to top and get next outer tuple.
        } while (true);
    }

    /**
     * implement the abstract method close() from super class Iterator to finish cleaning up
     * 
     * @exception IOException    I/O error from lower layers
     * @exception JoinsException join error from lower layers
     * @exception IndexException index access error
     */
    public void close() throws JoinsException, IOException, IndexException {
        if (!closeFlag) {

            try {
                outer.close();
            } catch (Exception e) {
                throw new JoinsException(e, "IEJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
