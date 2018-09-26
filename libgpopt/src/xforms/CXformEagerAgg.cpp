//---------------------------------------------------------------------------i
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CXformEagerAgg.cpp
//
//	@doc:
//		Implementation for eagerly pushing aggregates below join when there is
//      no primary/foreign keys
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformEagerAgg.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformEagerAgg::CXformEagerAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformEagerAgg::CXformEagerAgg
	(
	IMemoryPool *mp
	)
	:
	// pattern
	CXformExploration
		(
		GPOS_NEW(mp) CExpression
			(
			mp,
			GPOS_NEW(mp) CLogicalGbAgg(mp),
			GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CLogicalInnerJoin(mp),
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // join outer child
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // join inner child
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // join predicate
				),
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))	 // scalar project list
			)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformEagerAgg::CXformEagerAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformEagerAgg::CXformEagerAgg
	(
	CExpression *pexprPattern
	)
	:
	CXformExploration(pexprPattern)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformEagerAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		we only push down global aggregates
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformEagerAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (!popGbAgg->FGlobal())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformEagerAgg::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformEagerAgg::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *, //pxfres,
	CExpression *pexpr
	)
	const
{
    /*
     GPOS_ASSERT(NULL != pxfctxt);
     GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
     GPOS_ASSERT(FCheckPattern(pexpr));
    */
    IMemoryPool *mp = pxfctxt->Pmp();
    GPOS_ASSERT(mp);
        
    
    
    if (!FApplicable(pexpr))
        return;
    
    CExpression *expr_orig_relational_child = (*pexpr)[0];
    CExpression *expr_orig_proj_list = (*pexpr)[1];
    CColRefSet *orig_proj_list_colrefset =  CDrvdPropScalar::GetDrvdScalarProps(expr_orig_proj_list->PdpDerive())->PcrsUsed();
    GPOS_ASSERT(orig_proj_list_colrefset);
    

    CLogicalGbAgg *lg_gb_agg_op = CLogicalGbAgg::PopConvert(pexpr->Pop());
    CColRefSet *grouping_colrefset = lg_gb_agg_op->PcrsLocalUsed();
    GPOS_ASSERT(grouping_colrefset);
    
    CExpression *expr_left_child = (*expr_orig_relational_child)[0];
    CExpression *expr_right_child = (*expr_orig_relational_child)[1];
    
    CColRefSet *left_child_colrefset =  CDrvdPropRelational::GetRelationalProperties(expr_left_child->PdpDerive())->PcrsOutput();
    CColRefSet *right_child_colrefset =  CDrvdPropRelational::GetRelationalProperties(expr_right_child->PdpDerive())->PcrsOutput();
    GPOS_ASSERT(left_child_colrefset);
    GPOS_ASSERT(right_child_colrefset);
    
    CColRefSetArray *left_child_colref_array = GPOS_NEW(mp) CColRefSetArray(mp);
    left_child_colref_array->Append(left_child_colrefset);
    BOOL left_child = CColRefSet::FCovered(left_child_colref_array, orig_proj_list_colrefset);
    
    CColRefSetArray *right_child_colref_array = GPOS_NEW(mp) CColRefSetArray(mp);
    right_child_colref_array->Append(right_child_colrefset);
    BOOL right_child = CColRefSet::FCovered(right_child_colref_array, orig_proj_list_colrefset);
    

    
    
    
   
   

    
//    } else
//    {
//        CAutoTrace at(mp);
//        at.Os() << "NOT APPLYING" << std::endl;
//        at.Os() << *pexpr;
//    }
    
//    CExpression *pexprResult = CXformUtils::PexprPushGbBelowJoin(mp, pexpr);
//    if (NULL != pexprResult)
//    {
//        // add alternative to results
//        pxfres->Add(pexprResult);
//    }
}

//---------------------------------------------------------------------------
//    @function:
//        CXformEagerAgg::FApplicable
//
//    @doc:
//        Check if the transform can be applied
//          Eager agg is currently applied only if following is true:
//                1. Inner join of two relations
//                2. Single SUM agg
//                3. Single input in the agg
//
//---------------------------------------------------------------------------
BOOL
CXformEagerAgg::FApplicable
(
 CExpression *pexpr
 )
const
{
    CExpression *pexprPrjList = (*pexpr)[1];
    
    // assuming just a single agg function
    if (pexprPrjList->Arity() > 1)
    {
        return false;
    }
    CExpression *pexprProjElem = (*pexprPrjList)[0];
    CExpression *pexprScalarAggFunc = (*pexprProjElem)[0];
    
    // obtain the oid of the scalar agg function and compare to the oid for
    // AggSum that applies to the expression input to the agg
    CScalarAggFunc *popScalarAggFunc = CScalarAggFunc::PopConvert(pexprScalarAggFunc->Pop());
    IMDId *pmdidAgg = popScalarAggFunc->MDId();  // oid of the query agg function
   
    if (pexprScalarAggFunc->Arity() > 1)
    {
        // currently only supporting single-input aggregates
        return false;
    }
    
    CExpression *pexprAggChild = (*pexprScalarAggFunc)[0];
    IMDId* pmdidAggChild = CScalar::PopConvert(pexprAggChild->Pop())->MdidType();
    COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
    CMDAccessor *md_accessor = poctxt->Pmda();
    // get oid of the SUM AGG that applies to this identifier
    IMDId *pmidIdentSumAgg = md_accessor->RetrieveType(pmdidAggChild)->GetMdidForAggType(IMDType::EaggSum);
    
    if (!pmdidAgg->Equals(pmidIdentSumAgg))
    {
        return false;
    }
    
    return true;
}



// EOF

