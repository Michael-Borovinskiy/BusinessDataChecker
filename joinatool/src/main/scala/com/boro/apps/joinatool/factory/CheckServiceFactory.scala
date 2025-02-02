package com.boro.apps.joinatool.factory

import com.boro.apps.joinatool.Codes
import com.boro.apps.joinatool.checkimpl.{CheckEqualOnVal, CheckHolder, CheckTypesEval}

/**
 * @author Michael-Borovinskiy
 *         02.02.2025
 */
object CheckServiceFactory {

  def getCheckService(codes: Codes): CheckHolder = {
    codes match {
            case Codes.TYPES_EVAL => new CheckTypesEval
            case Codes.EQUAL_ON_VAL => new CheckEqualOnVal
            case _ => throw new UnsupportedOperationException("Check codeName hasn't any implemetation")
    }

  }



}
