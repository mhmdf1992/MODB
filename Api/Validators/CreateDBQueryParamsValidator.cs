using FluentValidation;
using MODB.Api.DTOs;

namespace MODB.Api.Validators{
    public class CreateDBQueryParamsValidator : AbstractValidator<CreateDBQueryParams>
    {
        public CreateDBQueryParamsValidator()
        {
            RuleFor(x => x.Name)
                .NotNull()
                    .WithErrorCode("ParameterMissing")
                    .WithMessage(x => $"{nameof(x.Name)} does not contain value")
                .NotEmpty()
                    .WithErrorCode("ParameterMissing")
                    .WithMessage(x => $"{nameof(x.Name)} does not contain value")
                .Matches("^[a-zA-Z0-9_.-]+$")
                    .WithErrorCode("InvalidFormat")
                    .WithMessage(x => $"{nameof(x.Name)} must match ^[a-zA-Z0-9_.-]+$");
        }
    }
}