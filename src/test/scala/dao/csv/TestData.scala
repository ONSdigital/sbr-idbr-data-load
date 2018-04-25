package dao.csv

import model.domain._

trait TestData {
  def testLocalUnit(local: Seq[LocalUnit]) = {
    Seq(
      LocalUnit("123123123", Some("50677559"), "2000000000", Some("9999999996"), "Tesco", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("63111"), Some("504.0")),
      LocalUnit("123123124", Some("50677559"), "2000000000", Some("9999999996"), "Tesco", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("63110"), Some("46.0")),
      LocalUnit("123123125", Some("50802921"), "2000000000", Some("9999999996"), "Tesco", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("63120"), Some("540.0")),
      LocalUnit("123123126", Some("20127938"), "7000000001", Some("9999999996"), "Test", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("41202"), Some("84.0")),
      LocalUnit("123123127", Some("50859981"), "7000000001", Some("9999999996"), "Test1", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("78109"), Some("517.0")),
      LocalUnit("123123128", Some("50837914"), "7000000001", Some("9999999996"), "Test2", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("69201"), Some("188.0")),
      LocalUnit("123123129", Some("12341234"), "1111111111", Some("9999999996"), "Test3", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("12312"), Some("100")),
      LocalUnit("123123110", Some("33322211"), "2000000001", Some("9999999996"), "Farm foods", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("46123"), Some("500")),
      LocalUnit("123123111", Some("55555555"), "2000000001", Some("9999999996"), "Farm foods", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("46444"), Some("301")),
      LocalUnit("123123112", Some("66666666"), "2000000001", Some("9999999996"), "Iceland", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("47544"), Some("500")),
      LocalUnit("123123113", Some("77777777"), "3000000001", Some("9999999996"), "Iceland", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("47144"), Some("1000")),
      LocalUnit("123123114", Some("88888888"), "3000000001", Some("9999999996"), "Iceland", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("47344"), Some("300")),
      LocalUnit("123123115", Some("99999999"), "3000000001", Some("9999999996"), "Iceland", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("47844"), Some("900")),
      LocalUnit("123123116", Some("11999999"), "3000000001", Some("9999999996"), "Iceland", Some("1"),"Address1", Some("2"), Some("3"), Some("4"), Some("5"), "postcode", Some("47944"), Some("950"))
    )
  }

  def testEnterprise(ent: Seq[Enterprise]) = {
    def getKeyByName(businessName:String): String = ent.collect{case Enterprise(ern,_, Some(`businessName`),_,_,_) => ern}.head

    Seq(
      Enterprise(getKeyByName("Tesco"), Some("1111111111"), Some("Tesco"), Some("R4 43L"), Some("1"), Some("")),
      Enterprise(getKeyByName("FakeComp"), Some("1000000000"), Some("FakeComp"), Some("F4 4K3"), Some("1"), Some("63111")),
      Enterprise(getKeyByName("Real Comp"), Some("1000000001"), Some("Real Comp"), Some("R4 43L"), Some("1"), Some("46123")),
      Enterprise(getKeyByName("Aldi"), Some("3000000003"), Some("Aldi"), Some("R4 43L"), Some("1"), Some("47144")),
      Enterprise(getKeyByName("Asda"), Some("7000000007"), Some("Asda"), Some("R4 43L"), Some("1"), Some("78109"))
    )
  }

  def testReportingUnit(reporting: Seq[ReportingUnit]) = {
    Seq(
      ReportingUnit("1", "1", "1", "1", "reu"),
      ReportingUnit("2", "2", "2", "2", "reu2")
    )
  }

}