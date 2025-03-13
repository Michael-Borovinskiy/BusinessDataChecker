package com.boro.apps.joinatool.factory

import com.boro.apps.joinatool.Codes
import com.boro.apps.joinatool.checkimpl.{CheckCountRows, CheckEqualOnVal, CheckHolder, CheckTypesEval}

/**
 * @author Michael-Borovinskiy
 *         02.02.2025
 */
object CheckServiceFactory {

  def getCheckImpl(codes: Codes): CheckHolder = {
    codes match {
            case Codes.TYPES_EVAL => new CheckTypesEval
            case Codes.EQUAL_ON_VAL => new CheckEqualOnVal
            case Codes.COUNT_ROWS => new CheckCountRows
            case _ => throw new UnsupportedOperationException("Check codeName doesn't have any implemetation")
    }

  }



}
