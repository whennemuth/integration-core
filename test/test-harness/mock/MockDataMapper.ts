import { DataMapper } from "../../../src/DataMapper"; 
import { Field, Input } from "../../../src/InputTypes";
import { RandomData } from "./RandomData";

export class MockDataMapper implements DataMapper {
  map(rawData: any): Input {
    const { fieldDefinitions } = RandomData;
    const getShortDate = (dte: Date | string): string => {
      if( dte instanceof Date ) {
        return dte.toISOString().split('T')[0];
      }
      else if(/^\d{4}\-\d{2}\-\d{2}/.test(dte)) {
        return dte.substring(0,10);
      }
      return dte;
    }
    const converted = {
      fieldDefinitions,
      fieldSets: rawData.map((rec: any) => ({
        fieldValues: [
          { fullname: `${rec.firstName} ${rec.middleInitial} ${rec.lastName}` },
          { dob: getShortDate(rec.dob) },
          { isActive: rec.deactivated === 'N' ? true : false },
          { signupDate: getShortDate(rec.memberSince) },
          { email: rec.emailAddress },
          { website: rec.website },
          { role: rec.role },
          { interests: rec.hobbies }
        ] satisfies Array<Field>
      }))
    } satisfies Input;
    return converted;
  };
}
