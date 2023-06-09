using FluentValidation;
using MODB.Api.DTOs;

namespace MODB.Api.Validators{
    public class SetKeyQueryParamsValidator : AbstractValidator<SetKeyQueryParams>
    {
        public SetKeyQueryParamsValidator()
        {
            RuleFor(x => x.Key)
                .NotNull()
                    .WithErrorCode("ParameterMissing")
                    .WithMessage(x => $"{nameof(x.Key)} does not contain value")
                .NotEmpty()
                    .WithErrorCode("ParameterMissing")
                    .WithMessage(x => $"{nameof(x.Key)} does not contain value")
                .Matches("^[a-zA-Z0-9_.-]+$")
                    .WithErrorCode("InvalidFormat")
                    .WithMessage(x => $"{nameof(x.Key)} must match ^[a-zA-Z0-9_.-]+$");
        }
    }
}