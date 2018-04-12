package dao.csv

import model.domain._

trait TestData {
  def testLocalUnit(local: Seq[LocalUnit]) = {
    def getKeyByName(ern:String): String = local.collect{case LocalUnit(lurn,_,`ern`,_,_,_,_,_,_,_,_,_,_,_) => lurn}.head

    Seq(
      LocalUnit(getKeyByName("9999999992"),Some("9999999996"),"9999999992",Some("1000000011"),"Tesco",Some("Retail"),"Address1",Some("2"),Some("3"),Some("4"),Some("5"),"postcode",Some("emp"),Some("")),
      LocalUnit(getKeyByName("9999999993"),Some("9999999997"),"9999999993",Some("1000000012"),"Farm foods",Some("Food"),"Address1",Some("2"),Some("3"),Some("4"),Some("5"),"postcode",Some("sic2"),Some("emp2")),
      LocalUnit(getKeyByName("9999999994"),Some("9999999998"),"9999999994",Some("1000000013"),"Newport school",Some("County"),"Address1",Some("2"),Some("3"),Some("4"),Some("5"),"postcode",Some("sic"),Some("emp")),
      LocalUnit(getKeyByName("9999999995"),Some("9999999999"),"9999999995",Some("1000000014"),"Warehouse",Some("Address1"),"2",Some("3"),Some("4"),Some("5"),Some("postcode"),"sic",Some("emp"),Some(""))
    )
  }

  def testEnterprise(ent: Seq[Enterprise]) = {
    def getKeyByName(businessName:String): String = ent.collect{case Enterprise(ern,_,Some(`businessName`),_,_) => ern}.head

    Seq(
      Enterprise(getKeyByName("FakeComp"),Some("1000000000"),Some("FakeComp"),Some("F4 4K3"),Some("1")),
      Enterprise(getKeyByName("Real Comp"),Some("1000000001"),Some("Real Comp"),Some("R4 43L"),Some("1"))
    )
  }
}