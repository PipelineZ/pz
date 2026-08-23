using System.Text.RegularExpressions;
using Pz.Connectors.TestKit;

/// <summary>Drives <see cref="Pz.Connectors.TestKit.ErrorRedactionContractTests"/> from both sides: a
/// redactor that understands the shapes real services answer in must pass it, and the specific redactor
/// this contract exists to catch — one that only understands <c>name=value</c> — must fail it.</summary>
public sealed class ErrorRedactionContractSelfTests
{
    /// <summary>Redacts the value of a <c>name=value</c> pair and the text of any XML element whose name
    /// suggests it carries a credential. Deliberately not clever: the contract is about covering the
    /// shapes, not about the redactor being sophisticated.</summary>
    private sealed class CoveringRedactor : Pz.Connectors.TestKit.ErrorRedactionContractTests
    {
        protected override string RedactErrorText(string thirdPartyMessage)
        {
            var redacted = Regex.Replace(
                thirdPartyMessage,
                @"<(AWSAccessKeyId|StringToSign)>.*?</\1>",
                "<$1>***</$1>",
                RegexOptions.Singleline | RegexOptions.IgnoreCase);

            return Regex.Replace(redacted, @"(?<=\b(?:Password|Signature|access_key)=)[^;'\s<]+", "***",
                RegexOptions.IgnoreCase);
        }
    }

    /// <summary>The redactor from the report: it understands <c>name=value</c> and nothing else, so the
    /// XML-element shape real object stores answer in walks straight past it.</summary>
    private sealed class NameValueOnlyRedactor : Pz.Connectors.TestKit.ErrorRedactionContractTests
    {
        protected override string RedactErrorText(string thirdPartyMessage) =>
            Regex.Replace(thirdPartyMessage, @"(?<==)[^;'\s<]+", "***");
    }

    /// <summary>The other failure mode: nothing leaks because nothing survives. A message naming nothing
    /// is not a diagnosis, and an artifact full of them is useless.</summary>
    private sealed class ErasesEverythingRedactor : Pz.Connectors.TestKit.ErrorRedactionContractTests
    {
        protected override string RedactErrorText(string thirdPartyMessage) => "***";
    }

    public static TheoryData<string, string, string> Shapes() =>
        Pz.Connectors.TestKit.ErrorRedactionContractTests.CredentialShapes();

    [Theory]
    [MemberData(nameof(Shapes))]
    public void A_covering_redactor_satisfies_every_shape(string shape, string payload, string diagnosis)
    {
        var sut = new CoveringRedactor();

        sut.Redaction_removes_the_credential_and_keeps_the_diagnosis(shape, payload, diagnosis);
    }

    [Fact]
    public void A_name_value_only_redactor_fails_the_xml_element_shape()
    {
        var sut = new NameValueOnlyRedactor();
        var (shape, payload, diagnosis) = FirstXmlElementShape();

        Assert.ThrowsAny<Exception>(
            () => sut.Redaction_removes_the_credential_and_keeps_the_diagnosis(shape, payload, diagnosis));
    }

    [Fact]
    public void A_redactor_that_erases_the_diagnosis_fails_too()
    {
        var sut = new ErasesEverythingRedactor();
        var (shape, payload, diagnosis) = FirstXmlElementShape();

        Assert.ThrowsAny<Exception>(
            () => sut.Redaction_removes_the_credential_and_keeps_the_diagnosis(shape, payload, diagnosis));
    }

    /// <summary>The s3 signature rejection — the case whose credential lives in an XML element rather
    /// than a name=value pair, which is the whole reason this contract exists.</summary>
    private static (string Shape, string Payload, string Diagnosis) FirstXmlElementShape()
    {
        var row = Pz.Connectors.TestKit.ErrorRedactionContractTests.CredentialShapes()
            .Select(r => ((string)r[0], (string)r[1], (string)r[2]))
            .First(r => r.Item1 == "s3 signature rejection");
        return row;
    }
}
