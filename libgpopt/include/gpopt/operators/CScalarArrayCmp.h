//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarArrayCmp.h
//
//	@doc:
//		Class for scalar array compare operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArrayCmp_H
#define GPOPT_CScalarArrayCmp_H

#include "gpos/base.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

namespace gpopt
{

	using namespace gpos;
	using namespace gpmd;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarArrayCmp
	//
	//	@doc:
	//		Class for scalar array compare operators
	//
	//---------------------------------------------------------------------------
	class CScalarArrayCmp : public CScalar
	{
		public:
		
			// type of array comparison
			enum EArrCmpType
			{
				EarrcmpAny,
				EarrcmpAll,
				EarrcmpSentinel
			};
			
		private:

			// compare operator mdid
			IMDId *m_mdid_op;

			// comparison operator name
			const CWStringConst *m_pscOp;
			
			// array compare type
			EArrCmpType m_earrccmpt;
			
			// length of the array constant
			INT m_array_length;

			// does operator return NULL on NULL input?
			BOOL m_returns_null_on_null_input;

			// private copy ctor
			CScalarArrayCmp(const CScalarArrayCmp &);
			
			// names of array compare types
			static
			const CHAR m_rgszCmpType[EarrcmpSentinel][10];

		public:
		
			// ctor
			CScalarArrayCmp
				(
				IMemoryPool *mp,
				IMDId *mdid_op,
				const CWStringConst *pstrOp,
				EArrCmpType earrcmpt,
				INT array_length
				);

			// dtor
			virtual 
			~CScalarArrayCmp()
			{
				m_mdid_op->Release();
				GPOS_DELETE(m_pscOp);
			}


			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopScalarArrayCmp;
			}
			
			// comparison type
			EArrCmpType Earrcmpt() const
			{
				return m_earrccmpt;
			}

			INT UlLength() const
			{
				return m_array_length;
			}

			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CScalarArrayCmp";
			}


			// operator specific hash function
			ULONG HashValue() const;
			
			// match function
			BOOL Matches(COperator *pop) const;
			
			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
			}
			
			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns
						(
						IMemoryPool *, //mp,
						UlongToColRefMap *, //colref_mapping,
						BOOL //must_exist
						)
			{
				return PopCopyDefault();
			}

			// conversion function
			static
			CScalarArrayCmp *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarArrayCmp == pop->Eopid());
				
				return reinterpret_cast<CScalarArrayCmp*>(pop);
			}

			// name of the comparison operator
			const CWStringConst *Pstr() const;

			// operator mdid
			IMDId *MdIdOp() const;
			
			// the type of the scalar expression
			virtual 
			IMDId *MdidType() const;

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const;

			// print
			virtual 
			IOstream &OsPrint(IOstream &os) const;

			// expand array comparison expression into a conjunctive/disjunctive expression
			static
			CExpression *PexprExpand(IMemoryPool *mp, CExpression *pexprArrayCmp);

	}; // class CScalarArrayCmp

}

#endif // !GPOPT_CScalarArrayCmp_H

// EOF
