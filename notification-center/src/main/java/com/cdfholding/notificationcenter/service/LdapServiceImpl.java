package com.cdfholding.notificationcenter.service;

import com.cdfholding.notificationcenter.domain.LdapInfo;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

public class LdapServiceImpl
    // implements LdapService
{

  private String ldap_server = "localhost:8081";

  //@Override
  public LdapInfo query(String adUser) {

    String LDAP_URL = "ldap://" + ldap_server + "/dc=example,dc=org";

    try {
      LdapContext ctx = new InitialLdapContext(null, null);
      ctx.setRequestControls(null);
      NamingEnumeration<?> namingEnum = ctx.search("ou=people,dc=example,dc=com",
          "(objectclass=user)", getSimpleSearchControls());
      while (namingEnum.hasMore()) {
        SearchResult result = (SearchResult) namingEnum.next();
        Attributes attrs = result.getAttributes();
        System.out.println(attrs.get("cn"));

      }
      namingEnum.close();
    } catch (Exception e) {

    }

    return null;
  }

  private SearchControls getSimpleSearchControls() {
    SearchControls searchControls = new SearchControls();
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setTimeLimit(30000);
    //String[] attrIDs = {"objectGUID"};
    //searchControls.setReturningAttributes(attrIDs);
    return searchControls;
  }
}
